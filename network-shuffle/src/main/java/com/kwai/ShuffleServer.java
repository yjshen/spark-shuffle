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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import org.apache.spark.network.TransportContext;
import org.apache.spark.network.server.TransportServer;
import org.apache.spark.network.server.TransportServerBootstrap;
import org.apache.spark.network.shuffle.ExternalShuffleBlockHandler;
import org.apache.spark.network.shuffle.cache.ExternalShuffleBlockHandlerWithCache;
import org.apache.spark.network.util.ServiceConf;
import org.apache.spark.network.util.TransportConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * An standalone external shuffle service used by Spark.
 * <p>
 * This is intended to be a long-running service that runs in the as a separate process per node.
 * A Spark application may connect to this service by setting `spark.shuffle.service.enabled`.
 * The application also automatically derives the service port through `spark.shuffle.service.port`
 * specified in the Yarn configuration. This is so that both the clients and the server agree on
 * the same port to communicate on.
 */
public class ShuffleServer {
    private static final Logger logger = LoggerFactory.getLogger(ShuffleServer.class);

    // just for testing when you want to find an open port
    @VisibleForTesting
    static int boundPort = -1;

    // just for integration tests that want to look at this file -- in general not sensible as
    // a static
    @VisibleForTesting
    static ShuffleServer instance;
    // Handles registering executors and opening shuffle blocks
    @VisibleForTesting
    ExternalShuffleBlockHandler blockHandler;
    // Handles registering executors and opening shuffle blocks with cache
    @VisibleForTesting
    ExternalShuffleBlockHandlerWithCache blockHandlerWithCache;
    MetricRegistry registry = new MetricRegistry();
    // The actual server that serves shuffle files
    private TransportServer shuffleServer = null;
    private ServiceConf _conf = null;

    public ShuffleServer() {
        logger.info("Initializing shuffle service for Spark");
        instance = this;
    }

    /**
     * Start the shuffle server with the given configuration.
     */
    protected void start(ServiceConf conf) throws Exception {
        _conf = conf;

        TransportConf transportConf = new TransportConf("shuffle", conf);

        // If authentication is enabled, set up the shuffle server to use a
        // special RPC handler that filters out unauthenticated fetch requests
        List<TransportServerBootstrap> bootstraps = Lists.newArrayList();

        boolean useBlockHandlerWithCache = conf.getCache().isEnabled();

        TransportContext transportContext;
        if (useBlockHandlerWithCache) {
            blockHandlerWithCache =
                new ExternalShuffleBlockHandlerWithCache(transportConf, registry);
            transportContext = new TransportContext(transportConf, blockHandlerWithCache);
        } else {
            blockHandler = new ExternalShuffleBlockHandler(transportConf);
            transportContext = new TransportContext(transportConf, blockHandler);
            registry.registerAll(blockHandler.getAllMetrics());
        }

        int port = conf.getPort();
        shuffleServer = transportContext.createServer(port, bootstraps);
        // the port should normally be fixed, but for tests its useful to find an open port
        port = shuffleServer.getPort();
        boundPort = port;

        registry.registerAll(shuffleServer.getAllMetrics());

        logger.info("Started shuffle service for Spark on port {}.", port);

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
        props.put("compression.type", "snappy");
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

    /**
     * Close the shuffle server to clean up any associated state.
     */
    protected void stop() {
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
        } catch (Exception e) {
            logger.error("Exception when stopping service", e);
        }
    }
}
