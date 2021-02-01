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

import static com.google.common.base.Preconditions.checkArgument;
import com.google.common.collect.Maps;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocatorMetric;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.spark.network.shuffle.cache.metrics.BlockManagerMonitor;
import org.apache.spark.network.util.JavaUtils;
import org.apache.spark.network.util.TransportConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class ShuffleSegmentCache {
    private static final Logger logger = LoggerFactory.getLogger(ShuffleSegmentCache.class);
    protected final long capacity;
    protected final ConcurrentMap<String, Long> cacheSizeByAppId;
    protected final BlockManagerMonitor monitor;
    protected final TransportConf conf;
    private final boolean useDirectMemory;
    private final AtomicBoolean seenOOM = new AtomicBoolean(false);
    private final ByteBufAllocatorMetric metric = UnpooledByteBufAllocator.DEFAULT.metric();
    private Object allocationLock = new Object();

    public ShuffleSegmentCache(long capacity, BlockManagerMonitor monitor, TransportConf conf) {
        this.capacity = capacity;
        this.monitor = monitor;
        this.cacheSizeByAppId = Maps.newConcurrentMap();
        this.conf = conf;
        this.useDirectMemory = conf.cachePreferDirect();
    }

    abstract ByteBuf get(ShuffleSegment segment) throws Exception;

    abstract void invalidateAll();

    abstract void invalidateAll(Iterable<ShuffleSegment> keys);

    abstract void invalidate(ShuffleSegment key);

    protected ByteBuf loadShuffleSegment(ShuffleSegment segment) throws IOException {
        checkArgument(segment.getCacheState() == ShuffleSegment.CacheState.CACHABLE,
            "Trying to cache a segment with wrong cache state: " + segment);
        ByteBuf segmentToCache = tryAllocate(segment.getLength());
        if (segmentToCache == null) {
            return null;
        }
        FileChannel channel = null;
        logger.debug("Start to load segment {} into cache", segment);
        long startTime = System.currentTimeMillis();
        try {
            channel = new RandomAccessFile(segment.getDataFile(), "r").getChannel();
            ByteBuffer mmaped =
                channel.map(FileChannel.MapMode.READ_ONLY, segment.getOffset(), segment.getLength());
            segmentToCache.writeBytes(mmaped);
            segment.setCacheState(ShuffleSegment.CacheState.CACHED);
            segment.loadedIntoCache();
            logger.debug("Load segment {} in to cache successfully in {} millis",
                segment, System.currentTimeMillis() - startTime);
            return segmentToCache;
        } catch (IOException e) {
            String errorMessage = "Error in reading " + segment;
            // We failed to load the segment into cache, set it to evicted to avoid it
            segment.setCacheState(ShuffleSegment.CacheState.EVICTED);
            try {
                if (channel != null) {
                    long size = channel.size();
                    errorMessage = "Error in reading " + segment + " (actual file length " + size + ")";
                }
            } catch (IOException ignored) {
                // ignore
            }
            logger.warn("Load segment {} in to cache failed due to {} in {} millis",
                segment, ExceptionUtils.getStackTrace(e), System.currentTimeMillis() - startTime);
            throw new IOException(errorMessage, e);
        } catch (OutOfMemoryError e) {
            seenOOM.set(true);
            return null;
        } finally {
            JavaUtils.closeQuietly(channel);
        }
    }

    protected void loadingSegment(ShuffleSegment segment) {
        cacheSizeByAppId.merge(segment.getAppId(), segment.getLength(),
            (oldValue, value) -> oldValue + value);
        monitor.segmentLoaded(segment);
    }

    protected void evictingSegment(ShuffleSegment segment) {
        cacheSizeByAppId.computeIfPresent(segment.getAppId(),
            (s, oldValue) -> oldValue - segment.getLength());
        cacheSizeByAppId.remove(segment.getAppId(), 0L);
        monitor.segmentEvicted(segment);
    }

    final boolean hasExceededQuota(String appId) {
        long appCachedSize = cacheSizeByAppId.getOrDefault(appId, 0L);
        int appNumInCache = cacheSizeByAppId.size() == 0 ? 1 : cacheSizeByAppId.size();
        return appCachedSize > capacity * 2 / appNumInCache;
    }

    public void applicationRemoved(String appId) {
        cacheSizeByAppId.remove(appId);
    }

    public void checkOODMErrorAndReset() {
        if (seenOOM.compareAndSet(true, false)) {
            logger.warn("OutOfMemory encountered and we are going to free the segment cache");
            invalidateAll();
        }
    }

    private ByteBuf tryAllocate(long length) {
        ByteBuf result = null;
        if (nettyMemoryUsage() + length <= capacity) {
            synchronized (allocationLock) {
                if (nettyMemoryUsage() + length <= capacity) {
                    result = getNettyBuf(length);
                }
            }
        }
        return result;
    }

    private long nettyMemoryUsage() {
        if (useDirectMemory) {
            return metric.usedDirectMemory();
        } else {
            return metric.usedHeapMemory();
        }
    }

    private ByteBuf getNettyBuf(long length) {
        if (useDirectMemory) {
            return Unpooled.directBuffer((int) length);
        } else {
            return Unpooled.buffer((int) length);
        }
    }
}
