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

package org.apache.spark.network.shuffle.cache.metrics;

import static java.util.Objects.requireNonNull;

import java.util.concurrent.atomic.LongAdder;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.github.benmanes.caffeine.cache.stats.StatsCounter;
import org.checkerframework.checker.index.qual.NonNegative;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link StatsCounter} instrumented with Dropwizard Metrics.
 */
public final class CaffeineCacheMetrics implements StatsCounter {
    private static final Logger logger = LoggerFactory.getLogger(CaffeineCacheMetrics.class);

    private final LongAdder hitCount = new LongAdder();
    private final LongAdder loadSuccess = new LongAdder();
    private final LongAdder loadFailure = new LongAdder();

    /**
     * Constructs an instance for use by a single cache.
     *
     * @param registry the registry of metric instances
     * @param metricsPrefix the prefix name for the metrics
     */
    public CaffeineCacheMetrics(MetricRegistry registry, String metricsPrefix) {
        requireNonNull(metricsPrefix);
        registry.register(MetricRegistry.name(metricsPrefix, "hits"),
                (Gauge<Long>) () -> hitCount.sumThenReset());
        registry.register(MetricRegistry.name(metricsPrefix, "loadsSuccess"),
                (Gauge<Long>) () -> loadSuccess.sumThenReset());
        registry.register(MetricRegistry.name(metricsPrefix, "loadsFailure"),
                (Gauge<Long>) () -> loadFailure.sumThenReset());
    }

    @Override
    public void recordHits(int count) {
        hitCount.add(count);
    }

    @Override
    public void recordMisses(int count) { }

    @Override
    public void recordLoadSuccess(long loadTime) {
        loadSuccess.add(loadTime);
    }

    @Override
    public void recordLoadFailure(long loadTime) {
        loadFailure.add(loadTime);
    }

    @Override
    @SuppressWarnings("deprecation")
    public void recordEviction() {
    }

    @Override
    public void recordEviction(int weight) {
    }

    @Override
    public void recordEviction(@NonNegative int weight, RemovalCause cause) {
        logger.debug("Segment weighted {} got evicted because of {}", weight, cause);
    }

    // we don't need this in kafka report
    @Override
    public CacheStats snapshot() {
        return new CacheStats(0L, 0L, 0L, 0L,0L, 0L,0L);
    }

    @Override
    public String toString() {
        return "";
    }
}
