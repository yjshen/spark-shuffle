package com.kwai;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import org.apache.spark.network.util.JavaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.io.IOException;
import java.time.Duration;

public class ServiceConf {
    private static Logger logger = LoggerFactory.getLogger(ServiceConf.class);

    private int port;
    private int thread;
    private boolean authenticate;
    private boolean directMemory;
    private ZKConf zookeeper;
    private CacheConf cache;
    private MetricsConf metrics;

    public ServiceConf() {}

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
            .add("port", port)
            .add("thread", thread)
            .add("authenticate", authenticate)
            .add("directMemory", directMemory)
            .add("zookeeper", zookeeper)
            .add("cache", cache)
            .add("metrics", metrics)
            .toString();
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getThread() {
        return thread;
    }

    public void setThread(int thread) {
        this.thread = thread;
    }

    public boolean isAuthenticate() {
        return authenticate;
    }

    public void setAuthenticate(boolean authenticate) {
        this.authenticate = authenticate;
    }

    public boolean isDirectMemory() {
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
        private String broker;
        private int port;

        public ZKConf() {}

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

        @Override
        public String toString() {
            return Objects.toStringHelper(this)
                .add("broker", broker)
                .add("port", port)
                .toString();
        }
    }

    public static class CacheConf {
        private boolean enabled;
        private String size;
        private boolean directMemory;
        private String evictTime;
        private String readThroughSize;
        private String impl;
        private boolean quotaEnabled;

        public CacheConf() {}

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

        public void setSize(String size) {
            this.size = size;
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

        public void setReadThroughSize(String readThroughSize) {
            this.readThroughSize = readThroughSize;
        }

        public void setImpl(String impl) {
            this.impl = impl;
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

        public Duration getEvictTime() {
            return Duration.ofSeconds(JavaUtils.timeStringAsSec(evictTime));
        }

        public long getReadThroughSize() {
            return JavaUtils.byteStringAsBytes(readThroughSize);
        }

        public String getImpl() {
            return impl;
        }
    }

    public static class MetricsConf {
        private String monitorLevel;
        private String reportInterval;
        private String kafkaBroker;
        private String kafkaTopic;
        private String histogramTimeWindow;

        public MetricsConf() {}

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

    public static ServiceConf parseConfFile(String confFile) {
        ServiceConf conf = null;
        try {
            File file = new File(confFile);
            ObjectMapper om = new ObjectMapper(new YAMLFactory());
            conf = om.readValue(file, ServiceConf.class);
        } catch (IOException e) {
            logger.error("Failed to parse core.yml due to: ", e);
        }

        Preconditions.checkNotNull(conf, "Failed to parse conf");
        Preconditions.checkNotNull(conf.zookeeper, "Failed to parse zookeeper conf");
        Preconditions.checkNotNull(conf.cache, "Failed to parse cache conf");
        Preconditions.checkNotNull(conf.metrics, "Failed to parse metrics conf");

        return conf;
    }
}
