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

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SlidingTimeWindowReservoir;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.spark.network.shuffle.cache.ShuffleSegment;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

public class GenericBlockManagerMonitor implements BlockManagerMonitor {

    private final MetricRegistry registry;

    private final LongAdder requestCount = new LongAdder();
    private final LongAdder segmentCachedSize = new LongAdder();
    private final LongAdder segmentCachedCount = new LongAdder();
    private final LongAdder segmentEvictedSize = new LongAdder();
    private final LongAdder segmentEvictedCount = new LongAdder();

    private final LongAdder readCachedCount = new LongAdder();
    private final LongAdder readThroughRequestCount = new LongAdder();
    private final LongAdder readThroughSegmentCount = new LongAdder();

    // total size since last report
    private final LongAdder readCachedSizeV = new LongAdder();
    private final LongAdder readThroughRequestSizeV = new LongAdder();
    private final LongAdder readThroughSegmentSizeV = new LongAdder();

    private final LongAdder cacheSegmentSizeTotal = new LongAdder();
    private final LongAdder cacheSegmentCountTotal = new LongAdder();

    private final Histogram segmentReadUsageAtEviction;
    private final Histogram readCachedSize;
    private final Histogram readThroughRequestSize;
    private final Histogram readThroughSegmentSize;

    public GenericBlockManagerMonitor(MetricRegistry registry, int histogramWindowSize) {
        this.registry = registry;
        // number of stats since last report
        registry.register("shuffleRequestNum", (Gauge<Long>) () -> requestCount.sumThenReset());
        registry.register("segmentCachedSize", (Gauge<Long>) () -> segmentCachedSize.sumThenReset());
        registry.register("segmentCachedCount", (Gauge<Long>) () -> segmentCachedCount.sumThenReset());
        registry.register("segmentEvictedSize", (Gauge<Long>) () -> segmentEvictedSize.sumThenReset());
        registry.register("segmentEvictedCount", (Gauge<Long>) () -> segmentEvictedCount.sumThenReset());

        registry.register("readCachedCount", (Gauge<Long>) () -> readCachedCount.sumThenReset());
        registry.register("readThroughRequestCount", (Gauge<Long>) () -> readThroughRequestCount.sumThenReset());
        registry.register("readThroughSegmentCount", (Gauge<Long>) () -> readThroughSegmentCount.sumThenReset());
        registry.register("readCachedSizeV", (Gauge<Long>) () -> readCachedSizeV.sumThenReset());
        registry.register("readThroughRequestSizeV", (Gauge<Long>) () -> readThroughRequestSizeV.sumThenReset());
        registry.register("readThroughSegmentSizeV", (Gauge<Long>) () -> readThroughSegmentSizeV.sumThenReset());

        registry.register("segmentTotalSizeInCache", (Gauge<Long>) () -> cacheSegmentSizeTotal.sum());
        registry.register("segmentTotalCountInCache", (Gauge<Long>) () -> cacheSegmentCountTotal.sum());

        registry.register("UnpooledAllocatorDirect", (Gauge<Long>) () -> UnpooledByteBufAllocator.DEFAULT.metric().usedDirectMemory());
        registry.register("UnpooledAllocatorHeap", (Gauge<Long>) () -> UnpooledByteBufAllocator.DEFAULT.metric().usedHeapMemory());

        segmentReadUsageAtEviction = registry.register("segmentUsageAtEviction",
                new Histogram(new SlidingTimeWindowReservoir(
                        histogramWindowSize, TimeUnit.MINUTES)));
        readCachedSize = registry.register("readCachedSize",
                new Histogram(new SlidingTimeWindowReservoir(
                        histogramWindowSize, TimeUnit.MINUTES)));
        readThroughRequestSize = registry.register("readThroughRequestSize",
                new Histogram(new SlidingTimeWindowReservoir(
                        histogramWindowSize, TimeUnit.MINUTES)));
        readThroughSegmentSize = registry.register("readThroughSegmentSize",
                new Histogram(new SlidingTimeWindowReservoir(
                        histogramWindowSize, TimeUnit.MINUTES)));
    }

    @Override
    public MetricRegistry getRegistry() {
        return registry;
    }

    @Override
    public void applicationRemoved(String appId) {
        // do nothing
    }

    @Override
    public void readRequest(String appId, long size) {
        requestCount.increment();
    }

    @Override
    public void readThroughRequest(String appId, long size) {
        readRequest(appId, size);
        readThroughRequestSize.update(size);
        readThroughRequestSizeV.add(size);
        readThroughRequestCount.increment();
    }

    @Override
    public void readThroughFromSegment(String appId, long size) {
        readRequest(appId, size);
        readThroughSegmentSize.update(size);
        readThroughSegmentSizeV.add(size);
        readThroughSegmentCount.increment();
    }

    @Override
    public void readCachedFromSegment(String appId, long size) {
        readRequest(appId, size);
        readCachedSize.update(size);
        readCachedSizeV.add(size);
        readCachedCount.increment();
    }

    @Override
    public void segmentLoaded(ShuffleSegment segment) {
        cacheSegmentSizeTotal.add(segment.getLength());
        cacheSegmentCountTotal.increment();
        segmentCachedSize.add(segment.getLength());
        segmentCachedCount.increment();
    }

    @Override
    public void segmentEvicted(ShuffleSegment segment) {
        cacheSegmentSizeTotal.add(- segment.getLength());
        cacheSegmentCountTotal.decrement();
        segmentEvictedSize.add(segment.getLength());
        segmentEvictedCount.increment();
        segmentReadUsageAtEviction.update((int)(segment.getUsageRatio() * 100));
    }
}
