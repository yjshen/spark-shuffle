package com.kwai;

import static scala.compat.java8.JFunction.*;

import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.Path;
import org.apache.spark.network.server.TransportServer;
import org.apache.spark.network.shuffle.ExternalShuffleBlockHandler;
import org.apache.spark.network.shuffle.cache.ExternalShuffleBlockHandlerWithCache;
import org.iq80.leveldb.DB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.compat.java8.JFunction0;
import scala.runtime.BoxedUnit;
import java.io.File;
import java.util.concurrent.CountDownLatch;

public class SparkShuffleServer {
    private static Logger logger = LoggerFactory.getLogger(SparkShuffleServer.class);
    private static CountDownLatch barrier = new CountDownLatch(1);

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

    private TransportServer server = null;

    public SparkShuffleServer() {

    }

    public void start() {
        Preconditions.checkState(server == null, "Shuffle server already started");
        logger.info("Starting shuffle service on port {}");
    }

    public void stop() {

    }

    public static void main(String[] args) throws InterruptedException {
        Utils.initDaemon(logger);
        SparkShuffleServer server = new SparkShuffleServer();
        server.start();

        logger.debug("Adding shutdown hook");
        ShutdownHookManager.addShutdownHook((JFunction0<BoxedUnit>) () -> {
            logger.info("Shutting down shuffle service.");
            server.stop();
            barrier.countDown();
            return null;
        });

        // keep running until the process is terminated
        barrier.await();
    }
}
