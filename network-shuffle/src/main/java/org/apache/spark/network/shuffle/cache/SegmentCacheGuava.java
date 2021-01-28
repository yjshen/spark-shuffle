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

package org.apache.spark.network.shuffle.cache;

import com.codahale.metrics.MetricRegistry;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.Weigher;
import io.netty.buffer.ByteBuf;
import org.apache.spark.network.shuffle.cache.metrics.BlockManagerMonitor;
import org.apache.spark.network.shuffle.cache.metrics.GuavaCacheMetrics;
import org.apache.spark.network.util.TransportConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class SegmentCacheGuava extends ShuffleSegmentCache {
    private static final Logger logger = LoggerFactory.getLogger(SegmentCacheGuava.class);

    private final LoadingCache<ShuffleSegment, ByteBuf> shuffleSegmentCache;

    public SegmentCacheGuava(
            long cacheCapacity,
            MetricRegistry registry,
            BlockManagerMonitor monitor,
            TransportConf conf) {
        super(cacheCapacity, monitor, conf);

        CacheLoader<ShuffleSegment, ByteBuf> shuffleCacheLoader =
            new CacheLoader<ShuffleSegment, ByteBuf>() {
                @Override
                public ByteBuf load(ShuffleSegment segment) throws Exception {
                    logger.debug("Loading segment {} into cache", segment);
                    ByteBuf result = loadShuffleSegment(segment);
                    loadingSegment(segment);
                    return result;
                }
            };

        RemovalListener<ShuffleSegment, ByteBuf> removalListener =
            notification -> {
                ShuffleSegment segment = notification.getKey();
                ByteBuf buffer = notification.getValue();
                logger.info("Segment got evicted, {}, usage ratio: [{} / {}], cause: {}, time since loaded: {}s, since last touched: {}s",
                    segment, segment.getTouchedNum(), segment.getNumNonEmptyPartitions(),
                    notification.getCause().name(), segment.timeSinceLoad(), segment.timeSinceLastTouched());
                long stamp = segment.getLock().writeLock();
                try {
                    segment.setCacheState(ShuffleSegment.CacheState.EVICTED);
                    buffer.release();
                    evictingSegment(segment);
                } finally {
                    segment.getLock().unlockWrite(stamp);
                }
            };

        CacheBuilder<ShuffleSegment, ByteBuf> builder = CacheBuilder.newBuilder()
            .maximumWeight(cacheCapacity)
            .weigher((Weigher<ShuffleSegment, ByteBuf>) (segment, buffer) ->
                (int) segment.getLength())
            .removalListener(removalListener)
            .recordStats();

        long expirationAfterAccess = conf.cacheEvictTimeSec();
        if (expirationAfterAccess > 0) {
            builder.expireAfterAccess(expirationAfterAccess, TimeUnit.SECONDS);
            logger.warn("Guava cache configured to evict shuffle segment after {} minutes since its last access",
                expirationAfterAccess);
        }

        shuffleSegmentCache = builder.build(shuffleCacheLoader);

        if (expirationAfterAccess > 0) {
            ScheduledExecutorService eagerCleaner = Executors.newSingleThreadScheduledExecutor();
            eagerCleaner.scheduleAtFixedRate(shuffleSegmentCache::cleanUp,
                expirationAfterAccess, expirationAfterAccess, TimeUnit.SECONDS);
        }

        registry.registerAll(
                GuavaCacheMetrics.metricsFor("guava", shuffleSegmentCache));
    }

    @Override
    public ByteBuf get(ShuffleSegment segment) throws Exception {
        return shuffleSegmentCache.get(segment);
    }

    @Override
    public void invalidateAll() {
        shuffleSegmentCache.invalidateAll();
    }

    @Override
    public void invalidateAll(Iterable<ShuffleSegment> keys) {
        shuffleSegmentCache.invalidateAll(keys);
    }

    @Override
    void invalidate(ShuffleSegment key) {
        shuffleSegmentCache.invalidate(key);
    }
}
