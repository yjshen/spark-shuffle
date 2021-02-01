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

import static com.codahale.metrics.MetricRegistry.name;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricSet;
import com.google.common.cache.Cache;
import java.util.HashMap;
import java.util.Map;

public class GuavaCacheMetrics extends HashMap<String, Metric> implements MetricSet {

    private GuavaCacheMetrics() {
    }

    /**
     * Wraps the provided Guava cache's statistics into Gauges suitable for reporting via Codahale Metrics
     * The returned MetricSet is suitable for registration with a MetricRegistry like so:
     * <code>registry.registerAll( GuavaCacheMetrics.metricsFor( "MyCache", cache ) );</code>
     *
     * @param cacheName This will be prefixed to all the reported metrics
     * @param cache     The cache from which to report the statistics
     * @return MetricSet suitable for registration with a MetricRegistry
     */
    public static MetricSet metricsFor(String cacheName, final Cache cache) {

        GuavaCacheMetrics metrics = new GuavaCacheMetrics();

        metrics.put(name(cacheName, "hitRate"),
            (Gauge<Double>) () -> cache.stats().hitRate());

        metrics.put(name(cacheName, "hitCount"),
            (Gauge<Long>) () -> cache.stats().hitCount());

        metrics.put(name(cacheName, "missCount"),
            (Gauge<Long>) () -> cache.stats().missCount());

        metrics.put(name(cacheName, "loadExceptionCount"),
            (Gauge<Long>) () -> cache.stats().loadExceptionCount());

        metrics.put(name(cacheName, "evictionCount"),
            (Gauge<Long>) () -> cache.stats().evictionCount());

        metrics.put(name(cacheName, "size"),
            (Gauge<Long>) () -> cache.size());

        return metrics;
    }

    @Override
    public Map<String, Metric> getMetrics() {
        return this;
    }

}
