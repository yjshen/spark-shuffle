package org.apache.spark.network.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.stream.Collectors;

public class ServiceConf {
    private static Logger logger = LoggerFactory.getLogger(ServiceConf.class);

    private int port = 9999;
    private int serverThreads = 156;
    private String mode = "NIO";
    private boolean directMemory = true;
    private String connectionTimeout = "2min";
    private int numConnectionsPerPeer = 1;
    private int backLog = -1;
    private int clientThreads = 0;
    private String memoryMapThreshold = "2m";
    private long maxChunksBeingTransferred = Long.MAX_VALUE;
    private int receiveBuffer = -1;
    private int sendBuffer = -1;
    private int maxRetries = 3;
    private String retryWait = "5s";
    private boolean lazyFD = true;
    private boolean enableVerboseMetrics = false;
    private String spark3ExecutorPath = "/home/var/lib/yarn/yarn-nm-recovery/nm-aux-services/spark3_shuffle";
    private String sparkaeExecutorPath = "/home/var/lib/yarn/yarn-nm-recovery/nm-aux-services/spark_adaptive_shuffle";

    private ZKConf zookeeper;
    private CacheConf cache;
    private MetricsConf metrics;

    public ServiceConf() {
    }

    public static ServiceConf getServiceConf() {
        ServiceConf sc = new ServiceConf();
        sc.setZookeeper(new ZKConf());
        sc.setCache(new CacheConf());
        sc.setMetrics(new MetricsConf());
        return sc;
    }

    public static ServiceConf parseConfFile(InputStream stream) {
        ServiceConf conf = null;
        try {
            ObjectMapper om = new ObjectMapper(new YAMLFactory());
            conf = om.readValue(stream, ServiceConf.class);
        } catch (IOException e) {
            logger.error("Failed to parse stream of core.yml due to: ", e);
        }

        Preconditions.checkNotNull(conf, "Failed to parse conf");
        Preconditions.checkNotNull(conf.zookeeper, "Failed to parse zookeeper conf");
        Preconditions.checkNotNull(conf.cache, "Failed to parse cache conf");
        Preconditions.checkNotNull(conf.metrics, "Failed to parse metrics conf");

        return conf;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
            .add("port", port)
            .add("zookeeper", zookeeper)
            .add("cache", cache)
            .add("metrics", metrics)
            .add("serverThreads", serverThreads)
            .add("mode", mode)
            .add("directMemory", directMemory)
            .add("connectionTimeout", connectionTimeout)
            .add("numConnectionsPerPeer", numConnectionsPerPeer)
            .add("backLog", backLog)
            .add("clientThreads", clientThreads)
            .add("memoryMapThreshold", memoryMapThreshold)
            .add("maxChunksBeingTransferred", maxChunksBeingTransferred)
            .add("receiveBuffer", receiveBuffer)
            .add("sendBuffer", sendBuffer)
            .add("maxRetries", maxRetries)
            .add("retryWait", retryWait)
            .add("lazyFD", lazyFD)
            .add("enableVerboseMetrics", enableVerboseMetrics)
            .add("aePath", sparkaeExecutorPath)
            .add("3path", spark3ExecutorPath)
            .toString();
    }

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    public int getConnectionTimeoutMs() {
        return Ints.checkedCast(JavaUtils.timeStringAsMs(connectionTimeout));
    }

    public void setConnectionTimeout(String connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
    }

    public int getNumConnectionsPerPeer() {
        return numConnectionsPerPeer;
    }

    public void setNumConnectionsPerPeer(int numConnectionsPerPeer) {
        this.numConnectionsPerPeer = numConnectionsPerPeer;
    }

    public int getClientThreads() {
        return clientThreads;
    }

    public void setClientThreads(int clientThreads) {
        this.clientThreads = clientThreads;
    }

    public int getReceiveBuffer() {
        return receiveBuffer;
    }

    public void setReceiveBuffer(int receiveBuffer) {
        this.receiveBuffer = receiveBuffer;
    }

    public int getSendBuffer() {
        return sendBuffer;
    }

    public void setSendBuffer(int sendBuffer) {
        this.sendBuffer = sendBuffer;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public void setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
    }

    public int getRetryWaitMs() {
        return Ints.checkedCast(JavaUtils.timeStringAsMs(retryWait));
    }

    public void setRetryWait(String retryWait) {
        this.retryWait = retryWait;
    }

    public boolean isLazyFD() {
        return lazyFD;
    }

    public void setLazyFD(boolean lazyFD) {
        this.lazyFD = lazyFD;
    }

    public boolean isEnableVerboseMetrics() {
        return enableVerboseMetrics;
    }

    public void setEnableVerboseMetrics(boolean enableVerboseMetrics) {
        this.enableVerboseMetrics = enableVerboseMetrics;
    }

    public long getMaxChunksBeingTransferred() {
        return maxChunksBeingTransferred;
    }

    public void setMaxChunksBeingTransferred(long maxChunksBeingTransferred) {
        this.maxChunksBeingTransferred = maxChunksBeingTransferred;
    }

    public int getMemoryMapThresholdBytes() {
        return Ints.checkedCast(JavaUtils.byteStringAsBytes(memoryMapThreshold));
    }

    public void setMemoryMapThreshold(String memoryMapThreshold) {
        this.memoryMapThreshold = memoryMapThreshold;
    }

    public String getSpark3ExecutorPath() {
        return spark3ExecutorPath;
    }

    public void setSpark3ExecutorPath(String spark3ExecutorPath) {
        this.spark3ExecutorPath = spark3ExecutorPath;
    }

    public String getSparkaeExecutorPath() {
        return sparkaeExecutorPath;
    }

    public void setSparkaeExecutorPath(String sparkaeExecutorPath) {
        this.sparkaeExecutorPath = sparkaeExecutorPath;
    }

    public int getBackLog() {
        return backLog;
    }

    public void setBackLog(int backLog) {
        this.backLog = backLog;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getServerThreads() {
        return serverThreads;
    }

    public void setServerThreads(int serverThreads) {
        this.serverThreads = serverThreads;
    }

    public boolean preferDirectMemory() {
        return directMemory;
    }

    public void setDirectMemory(boolean directMemory) {
        this.directMemory = directMemory;
    }

    public ZKConf getZookeeper() {
        return zookeeper;
    }

    public void setZookeeper(ZKConf zookeeper) {
        this.zookeeper = zookeeper;
    }

    public CacheConf getCache() {
        return cache;
    }

    public void setCache(CacheConf cache) {
        this.cache = cache;
    }

    public MetricsConf getMetrics() {
        return metrics;
    }

    public void setMetrics(MetricsConf metrics) {
        this.metrics = metrics;
    }

    public static class ZKConf {
        private String broker = "UNKNOWN";
        private int port = 2181;

        public ZKConf() {
        }

        public String getBroker() {
            return broker;
        }

        public void setBroker(String broker) {
            this.broker = broker;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }

        public String getHostPort() {
            return Arrays.stream(broker.split(","))
                .map(q -> q + ":" + port)
                .collect(Collectors.joining(","));
        }

        @Override
        public String toString() {
            return Objects.toStringHelper(this)
                .add("broker", broker)
                .add("port", port)
                .toString();
        }
    }

    public static class CacheConf {
        private boolean enabled = true;
        private String size = "10g";
        private boolean directMemory = true;
        private String evictTime = "30min";
        private String readThroughSize = "1m";
        private String impl = "caffeine";
        private boolean quotaEnabled = true;

        public CacheConf() {
        }

        @Override
        public String toString() {
            return Objects.toStringHelper(this)
                .add("enabled", enabled)
                .add("size", size)
                .add("directMemory", directMemory)
                .add("evictTime", evictTime)
                .add("readThroughSize", readThroughSize)
                .add("impl", impl)
                .add("quotaEnabled", quotaEnabled)
                .toString();
        }

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public boolean isDirectMemory() {
            return directMemory;
        }

        public void setDirectMemory(boolean directMemory) {
            this.directMemory = directMemory;
        }

        public void setEvictTime(String evictTime) {
            this.evictTime = evictTime;
        }

        public boolean isQuotaEnabled() {
            return quotaEnabled;
        }

        public void setQuotaEnabled(boolean quotaEnabled) {
            this.quotaEnabled = quotaEnabled;
        }

        public long getSize() {
            return JavaUtils.byteStringAsBytes(size);
        }

        public void setSize(String size) {
            this.size = size;
        }

        public long getEvictTimeSec() {
            return JavaUtils.timeStringAsSec(evictTime);
        }

        public long getReadThroughSize() {
            return JavaUtils.byteStringAsBytes(readThroughSize);
        }

        public void setReadThroughSize(String readThroughSize) {
            this.readThroughSize = readThroughSize;
        }

        public String getImpl() {
            return impl;
        }

        public void setImpl(String impl) {
            this.impl = impl;
        }
    }

    public static class MetricsConf {
        private String monitorLevel = "none";
        private String reportInterval = "1min";
        private String kafkaBroker = "UNKNOWN";
        private String kafkaTopic = "UNKNOWN";
        private String histogramTimeWindow = "1min";

        public MetricsConf() {
        }

        @Override
        public String toString() {
            return Objects.toStringHelper(this)
                .add("monitorLevel", monitorLevel)
                .add("reportInterval", reportInterval)
                .add("kafkaBroker", kafkaBroker)
                .add("kafkaTopic", kafkaTopic)
                .add("histogramTimeWindow", histogramTimeWindow)
                .toString();
        }

        public long getReportIntervalSec() {
            return JavaUtils.timeStringAsSec(reportInterval);
        }

        public long getHistogramWindowSec() {
            return JavaUtils.timeStringAsSec(histogramTimeWindow);
        }

        public String getMonitorLevel() {
            return monitorLevel;
        }

        public void setMonitorLevel(String monitorLevel) {
            this.monitorLevel = monitorLevel;
        }

        public void setReportInterval(String reportInterval) {
            this.reportInterval = reportInterval;
        }

        public String getKafkaBroker() {
            return kafkaBroker;
        }

        public void setKafkaBroker(String kafkaBroker) {
            this.kafkaBroker = kafkaBroker;
        }

        public String getKafkaTopic() {
            return kafkaTopic;
        }

        public void setKafkaTopic(String kafkaTopic) {
            this.kafkaTopic = kafkaTopic;
        }

        public void setHistogramTimeWindow(String histogramTimeWindow) {
            this.histogramTimeWindow = histogramTimeWindow;
        }
    }
}
