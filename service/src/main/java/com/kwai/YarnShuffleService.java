/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kwai;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.network.TransportContext;
import org.apache.spark.network.server.TransportServer;
import org.apache.spark.network.server.TransportServerBootstrap;
import org.apache.spark.network.shuffle.ExternalShuffleBlockHandler;
import org.apache.spark.network.shuffle.cache.ExternalShuffleBlockHandlerWithCache;
import org.apache.spark.network.util.LevelDBProvider;
import org.apache.spark.network.util.ServiceConf;
import org.apache.spark.network.util.TransportConf;
import org.iq80.leveldb.DB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * An external shuffle service used by Spark on Yarn.
 *
 * This is intended to be a long-running auxiliary service that runs in the NodeManager process.
 * A Spark application may connect to this service by setting `spark.shuffle.service.enabled`.
 * The application also automatically derives the service port through `spark.shuffle.service.port`
 * specified in the Yarn configuration. This is so that both the clients and the server agree on
 * the same port to communicate on.
 *
 * The service also optionally supports authentication. This ensures that executors from one
 * application cannot read the shuffle files written by those from another. This feature can be
 * enabled by setting `spark.authenticate` in the Yarn configuration before starting the NM.
 * Note that the Spark application must also set `spark.authenticate` manually and, unlike in
 * the case of the service port, will not inherit this setting from the Yarn configuration. This
 * is because an application running on the same Yarn cluster may choose to not use the external
 * shuffle service, in which case its setting of `spark.authenticate` should be independent of
 * the service's.
 */
public class YarnShuffleService {
  private static final Logger logger = LoggerFactory.getLogger(YarnShuffleService.class);

  // Port on which the shuffle server listens for fetch requests
  private static final String SPARK_SHUFFLE_SERVICE_PORT_KEY = "spark.adaptive.shuffle.service.port";
  private static final int DEFAULT_SPARK_SHUFFLE_SERVICE_PORT = 7337;

  // Whether the shuffle server should authenticate fetch requests
  private static final String SPARK_AUTHENTICATE_KEY = "spark.authenticate";
  private static final boolean DEFAULT_SPARK_AUTHENTICATE = false;

  private static final String RECOVERY_FILE_NAME = "registeredExecutors.ldb";
  private static final String SECRETS_RECOVERY_FILE_NAME = "sparkShuffleRecovery.ldb";

  // Whether failure during service initialization should stop the NM.
  @VisibleForTesting
  static final String STOP_ON_FAILURE_KEY = "spark.yarn.shuffle.stopOnFailure";
  private static final boolean DEFAULT_STOP_ON_FAILURE = false;

  // Whether we should use block handler with cache.
  @VisibleForTesting
  static final String BLOCK_HANDLER_WITH_CACHE_KEY = "spark.shuffle.cache.enabled";
  private static final boolean DEFAULT_BLOCK_HANDLER_WITH_CACHE = false;

  // just for testing when you want to find an open port
  @VisibleForTesting
  static int boundPort = -1;
  private static final ObjectMapper mapper = new ObjectMapper();
  private static final String APP_CREDS_KEY_PREFIX = "AppCreds";
  private static final LevelDBProvider.StoreVersion CURRENT_VERSION = new LevelDBProvider
      .StoreVersion(1, 0);

  // just for integration tests that want to look at this file -- in general not sensible as
  // a static
  @VisibleForTesting
  static YarnShuffleService instance;

  // The actual server that serves shuffle files
  private TransportServer shuffleServer = null;

  private ServiceConf _conf = null;

  // The recovery path used to shuffle service recovery
  @VisibleForTesting
  Path _recoveryPath = null;

  // Handles registering executors and opening shuffle blocks
  @VisibleForTesting
  ExternalShuffleBlockHandler blockHandler;

  // Handles registering executors and opening shuffle blocks with cache
  @VisibleForTesting
  ExternalShuffleBlockHandlerWithCache blockHandlerWithCache;

  // Where to store & reload executor info for recovering state after an NM restart
  @VisibleForTesting
  File registeredExecutorFile;

  // Where to store & reload application secrets for recovering state after an NM restart
  @VisibleForTesting
  File secretsFile;

  private DB db;

  MetricRegistry registry = new MetricRegistry();

  public YarnShuffleService() {
    logger.info("Initializing YARN shuffle service for Spark");
    instance = this;
  }

  /**
   * Start the shuffle server with the given configuration.
   */
  protected void serviceInit(ServiceConf conf) throws Exception {
    _conf = conf;

    // In case this NM was killed while there were running spark applications, we need to restore
    // lost state for the existing executors. We look for an existing file in the NM's local dirs.
    // If we don't find one, then we choose a file to use to save the state next time.  Even if
    // an application was stopped while the NM was down, we expect yarn to call stopApplication()
    // when it comes back
    if (_recoveryPath != null) {
      registeredExecutorFile = initRecoveryDb(RECOVERY_FILE_NAME);
    }

    TransportConf transportConf = new TransportConf("shuffle", conf);

    // If authentication is enabled, set up the shuffle server to use a
    // special RPC handler that filters out unauthenticated fetch requests
    List<TransportServerBootstrap> bootstraps = Lists.newArrayList();

    boolean useBlockHandlerWithCache = conf.getCache().isEnabled();

    TransportContext transportContext;
    if (useBlockHandlerWithCache) {
      blockHandlerWithCache =
          new ExternalShuffleBlockHandlerWithCache(transportConf, registeredExecutorFile, registry);
      transportContext = new TransportContext(transportConf, blockHandlerWithCache);
    } else {
      blockHandler = new ExternalShuffleBlockHandler(transportConf, registeredExecutorFile);
      transportContext = new TransportContext(transportConf, blockHandler);
      registry.registerAll(blockHandler.getAllMetrics());
    }

    int port = conf.getPort();
    shuffleServer = transportContext.createServer(port, bootstraps);
    // the port should normally be fixed, but for tests its useful to find an open port
    port = shuffleServer.getPort();
    boundPort = port;

    registry.registerAll(shuffleServer.getAllMetrics());

    logger.info("Started YARN shuffle service for Spark on port {}. " +
      "Registered executor file is {}", port, registeredExecutorFile);

    startMetricsReport();
  }

  private void startMetricsReport() {
    String kafkaBroker = _conf.getMetrics().getKafkaBroker();
    String kafkaTopic = _conf.getMetrics().getKafkaTopic();
    long metricReportIntervalSec = _conf.getMetrics().getReportIntervalSec();

    if (kafkaBroker.isEmpty() || kafkaTopic.isEmpty()) {
      logger.warn("Shuffle service metrics reporting is not properly configured:" +
              " KafkaBroker: {}, kafkaTopic: {}", kafkaBroker, kafkaTopic);
      return;
    }

    Properties props = new Properties();
    props.put("bootstrap.servers", kafkaBroker);
    props.put("acks", "0");
    props.put("compression.type","snappy");
    props.put("batch.size", 8096);
    props.put("linger.ms", 100);
    props.put("retries", 20);
    props.put("buffer.memory", 67108864);
    props.put("key.serializer", org.apache.kafka.common.serialization.StringSerializer.class);
    props.put("value.serializer", org.apache.kafka.common.serialization.StringSerializer.class);

    KafkaMetricsReporter reporter = KafkaMetricsReporter
            .forRegistry(registry)
            .config(props)
            .topic(kafkaTopic)
            .version("spark-ae-cache-1")
            .port(boundPort)
            .build();
    reporter.start(metricReportIntervalSec, TimeUnit.SECONDS);
  }

  public void initializeApplication(String appId) {
  }

  public void stopApplication(String appId) {
    try {
      if (blockHandler != null) {
        blockHandler.applicationRemoved(appId, false /* clean up local dirs */);
      }
      if (blockHandlerWithCache != null) {
        blockHandlerWithCache.applicationRemoved(appId, false /* clean up local dirs */);
      }
    } catch (Exception e) {
      logger.error("Exception when stopping application {}", appId, e);
    }
  }

  /**
   * Close the shuffle server to clean up any associated state.
   */
  protected void serviceStop() {
    try {
      if (shuffleServer != null) {
        shuffleServer.close();
      }
      if (blockHandler != null) {
        blockHandler.close();
      }
      if (blockHandlerWithCache != null) {
        blockHandlerWithCache.close();
      }
      if (db != null) {
        db.close();
      }
    } catch (Exception e) {
      logger.error("Exception when stopping service", e);
    }
  }

  /**
   * Set the recovery path for shuffle service recovery when NM is restarted. This will be call
   * by NM if NM recovery is enabled.
   */
  public void setRecoveryPath(Path recoveryPath) {
    _recoveryPath = recoveryPath;
  }

  /**
   * Get the path specific to this auxiliary service to use for recovery.
   */
  protected Path getRecoveryPath(String fileName) {
    return _recoveryPath;
  }

  /**
   * Figure out the recovery path and handle moving the DB if YARN NM recovery gets enabled
   * and DB exists in the local dir of NM by old version of shuffle service.
   */
  protected File initRecoveryDb(String dbName) {
    Preconditions.checkNotNull(_recoveryPath,
      "recovery path should not be null if NM recovery is enabled");

    File recoveryFile = new File(_recoveryPath.toUri().getPath(), dbName);
    if (recoveryFile.exists()) {
      return recoveryFile;
    }

    // db doesn't exist in recovery path go check local dirs for it
    String[] localDirs = _conf.getTrimmedStrings("yarn.nodemanager.local-dirs");
    for (String dir : localDirs) {
      File f = new File(new Path(dir).toUri().getPath(), dbName);
      if (f.exists()) {
        // If the recovery path is set then either NM recovery is enabled or another recovery
        // DB has been initialized. If NM recovery is enabled and had set the recovery path
        // make sure to move all DBs to the recovery path from the old NM local dirs.
        // If another DB was initialized first just make sure all the DBs are in the same
        // location.
        Path newLoc = new Path(_recoveryPath, dbName);
        Path copyFrom = new Path(f.toURI());
        if (!newLoc.equals(copyFrom)) {
          logger.info("Moving " + copyFrom + " to: " + newLoc);
          try {
            // The move here needs to handle moving non-empty directories across NFS mounts
            FileSystem fs = FileSystem.getLocal(_conf);
            fs.rename(copyFrom, newLoc);
          } catch (Exception e) {
            // Fail to move recovery file to new path, just continue on with new DB location
            logger.error("Failed to move recovery file {} to the path {}",
              dbName, _recoveryPath.toString(), e);
          }
        }
        return new File(newLoc.toUri().getPath());
      }
    }

    return new File(_recoveryPath.toUri().getPath(), dbName);
  }

}
