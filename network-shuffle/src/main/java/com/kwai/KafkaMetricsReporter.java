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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Timer;
import com.codahale.metrics.json.MetricsModule;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.SortedMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class KafkaMetricsReporter extends ScheduledReporter {
    private static final Logger logger = LoggerFactory.getLogger(KafkaMetricsReporter.class);
    private static final DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

    private final String topic;
    private final Producer<String, String> producer;
    private final ExecutorService kafkaExecutor;
    private final String hostName;
    private final String ip;
    private final String version;
    private final int port;

    private final ObjectMapper mapper;

    private KafkaMetricsReporter(MetricRegistry registry,
                                 String name,
                                 TimeUnit rateUnit,
                                 TimeUnit durationUnit,
                                 boolean showSamples,
                                 MetricFilter filter,
                                 String topic, Properties properties,
                                 String hostName, String ip,
                                 String version, int port) {
        super(registry, name, filter, rateUnit, durationUnit);
        this.topic = topic;
        this.hostName = hostName;
        this.ip = ip;
        this.version = version;
        this.port = port;
        this.mapper = new ObjectMapper().registerModule(
            new MetricsModule(rateUnit, durationUnit, showSamples));
        producer = new KafkaProducer<>(properties);
        kafkaExecutor = Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder().setNameFormat("Metrics-reporter-kafka-%d").build());
    }

    public static Builder forRegistry(MetricRegistry registry) {
        return new Builder(registry);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void report(SortedMap<String, Gauge> gauges,
                       SortedMap<String, Counter> counters,
                       SortedMap<String, Histogram> histograms,
                       SortedMap<String, Meter> meters,
                       SortedMap<String, Timer> timers) {

        final Map<String, Object> result = new HashMap<>();

        result.put("hostName", hostName);
        result.put("ip", ip);
        result.put("version", version);
        result.put("port", port);
        result.put("ts", System.currentTimeMillis());
        result.put("rateUnit", getRateUnit());
        result.put("durationUnit", getDurationUnit());

        result.putAll(gauges);
        result.putAll(counters);
        result.putAll(histograms);
        result.putAll(meters);
        result.putAll(timers);

        kafkaExecutor.execute(() -> {
            try {
                ProducerRecord<String, String> message = new ProducerRecord<>(
                    topic, mapper.writeValueAsString(result));
                producer.send(message);
                producer.flush();
            } catch (Exception e) {
                logger.error("Failed to send metrics to kafka at {} caused by {}",
                    dateFormat.format(Calendar.getInstance().getTime()),
                    ExceptionUtils.getStackTrace(e));
            }
        });
    }

    @Override
    public void close() {
        producer.flush();
        producer.close();
        super.close();
    }

    public static class Builder {
        private final MetricRegistry registry;
        private String name = "kafka-reporter";
        private String hostName;
        private String ip;
        private String topic;
        private String version;
        private int port;
        private Properties properties;

        private TimeUnit rateUnit;
        private TimeUnit durationUnit;
        private boolean showSamples;
        private MetricFilter filter;

        public Builder(MetricRegistry registry) {
            this.registry = registry;
            this.rateUnit = TimeUnit.SECONDS;
            this.durationUnit = TimeUnit.MILLISECONDS;
            this.filter = MetricFilter.ALL;
        }

        /**
         * Convert rates to the given time unit.
         *
         * @param rateUnit a unit of time
         * @return {@code this}
         */
        public Builder convertRatesTo(TimeUnit rateUnit) {
            this.rateUnit = rateUnit;
            return this;
        }

        /**
         * Convert durations to the given time unit.
         *
         * @param durationUnit a unit of time
         * @return {@code this}
         */
        public Builder convertDurationsTo(TimeUnit durationUnit) {
            this.durationUnit = durationUnit;
            return this;
        }

        public Builder showSamples(boolean showSamples) {
            this.showSamples = showSamples;
            return this;
        }

        /**
         * Only report metrics which match the given filter.
         *
         * @param filter a {@link MetricFilter}
         * @return {@code this}
         */
        public Builder filter(MetricFilter filter) {
            this.filter = filter;
            return this;
        }

        /**
         * default register name is "kafka-reporter".
         *
         * @param name
         * @return
         */
        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder topic(String topic) {
            this.topic = topic;
            return this;
        }

        public Builder config(Properties properties) {
            this.properties = properties;
            return this;
        }

        public Builder version(String version) {
            this.version = version;
            return this;
        }

        public Builder port(int port) {
            this.port = port;
            return this;
        }

        public Builder hostName(String hostName) {
            this.hostName = hostName;
            return this;
        }

        public Builder ip(String ip) {
            this.ip = ip;
            return this;
        }

        /**
         * Builds a {@link KafkaMetricsReporter} with the given properties.
         *
         * @return a {@link KafkaMetricsReporter}
         */
        public KafkaMetricsReporter build() {
            if (hostName == null) {
                hostName = HostUtil.getHostName();
                logger.info(name + " detect hostName: " + hostName);
            }
            if (ip == null) {
                ip = HostUtil.getHostAddress();
                logger.info(name + " detect ip: " + ip);
            }
            return new KafkaMetricsReporter(registry, name, rateUnit, durationUnit, showSamples,
                filter, topic, properties, hostName, ip, version, port);
        }
    }

    static class HostUtil {
        private static final Logger logger = LoggerFactory.getLogger(HostUtil.class);

        static String getHostName() {
            String host;
            try {
                host = InetAddress.getLocalHost().getHostName();
                if (host != null && !host.isEmpty()) {
                    return host;
                }
            } catch (UnknownHostException e) {
                logger.warn("Failed to get host name through InetAddress", e);
            }

            host = System.getenv("HOSTNAME");
            return host;
        }

        static String getHostAddress() {
            try {
                return InetAddress.getLocalHost().getHostAddress();
            } catch (UnknownHostException e) {
                logger.error("Failed to get host address", e);
            }
            return null;
        }
    }
}
