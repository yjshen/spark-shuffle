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
import com.carrotsearch.hppc.IntHashSet;
import com.google.common.base.Objects;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.Range;
import java.io.File;
import java.util.concurrent.locks.StampedLock;

public class ShuffleSegment {

    public static final ShuffleSegment DUMMY_SEGMENT =
        new ShuffleSegment("", null, null, 0, 1, -1, -1, null);
    private final String appId;
    private final File indexFile;
    private final File dataFile;
    private final int startPartitionId;
    private final int numPartitions;
    private final int numNonEmptyPartitions;
    private final Range<Integer> partitionRange;

    private final long offset;
    private final long length;
    private final IntHashSet unTouchedPartitions;
    private final StampedLock lock;
    private CacheState cacheState;
    private long loadTime = -1;
    private long lastTouchedTime = -1;

    public ShuffleSegment(
        String appId,
        File indexFile,
        File dataFile,
        int startPartitionId,
        int numPartitions,
        long offset,
        long length,
        IntHashSet allNonEmptyPartitions) {
        checkArgument(startPartitionId >= 0, "Start block idx cannot be negative for shuffle segment");
        checkArgument(numPartitions > 0, "Num of blocks cannot be negative or zero for shuffle segment");

        this.appId = appId;
        this.indexFile = indexFile;
        this.dataFile = dataFile;
        this.startPartitionId = startPartitionId;
        this.numPartitions = numPartitions;
        this.partitionRange = Range.closed(startPartitionId, startPartitionId + numPartitions - 1);
        this.offset = offset;
        this.length = length;
        this.lock = new StampedLock();
        this.unTouchedPartitions = allNonEmptyPartitions;
        this.numNonEmptyPartitions = unTouchedPartitions == null ? 0 : unTouchedPartitions.size();

        if (numNonEmptyPartitions > 2) {
            this.cacheState = CacheState.CACHABLE;
        } else {
            this.cacheState = CacheState.READ_THROUGH;
        }
    }

    /**
     * Convert a quantity in bytes to a human-readable string such as "4.0 MB".
     */
    private static String bytesToString(long size) {
        long GB = 1 << 30;
        long MB = 1 << 20;
        long KB = 1 << 10;
        double value;
        String unit;
        if (size >= 2 * GB) {
            value = (double) size / GB;
            unit = "GB";
        } else if (size >= 2 * MB) {
            value = (double) size / MB;
            unit = "MB";
        } else if (size >= 2 * KB) {
            value = (double) size / KB;
            unit = "KB";
        } else {
            value = size;
            unit = "B";
        }
        return String.format("%.1f %s", value, unit);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ShuffleSegment)) return false;
        ShuffleSegment that = (ShuffleSegment) o;
        return startPartitionId == that.startPartitionId &&
            numPartitions == that.numPartitions &&
            Objects.equal(indexFile, that.indexFile);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(indexFile, startPartitionId, numPartitions);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
            .add("App", appId)
            .add("indexFile", indexFile)
            .add("cacheState", cacheState)
            .add("startPartitionId", startPartitionId)
            .add("numPartitions", numPartitions)
            .add("numNonEmptyPartitions", numNonEmptyPartitions)
            .add("offset", offset)
            .add("length", bytesToString(length))
            .add("touched", unTouchedPartitions == null ? "null" : getTouchedNum())
            .toString();
    }

    public double getUsageRatio() {
        return ((double) getTouchedNum()) / numNonEmptyPartitions;
    }

    public String getAppId() {
        return appId;
    }

    public File getDataFile() {
        return dataFile;
    }

    public File getIndexFile() {
        return indexFile;
    }

    public int getStartPartitionId() {
        return startPartitionId;
    }

    public int getNumPartitions() {
        return numPartitions;
    }

    public int getNumNonEmptyPartitions() {
        return numNonEmptyPartitions;
    }

    public int getCurrentSize() {
        return unTouchedPartitions == null ? 0 : unTouchedPartitions.size();
    }

    public long getOffset() {
        return offset;
    }

    public long getLength() {
        return length;
    }

    public CacheState getCacheState() {
        return cacheState;
    }

    public void setCacheState(CacheState cacheState) {
        this.cacheState = cacheState;
    }

    public int getTouchedNum() {
        return numNonEmptyPartitions - getCurrentSize();
    }

    public StampedLock getLock() {
        return lock;
    }

    public void setTouched(Range<Integer> parts) {
        if (cacheState == CacheState.CACHED && partitionRange.isConnected(parts)) {
            Range<Integer> containedRange = partitionRange.intersection(parts);
            synchronized (this) {
                for (int partitionId : ContiguousSet.create(containedRange, DiscreteDomain.integers())) {
                    unTouchedPartitions.remove(partitionId);
                }
                lastTouchedTime = System.currentTimeMillis();
            }
        }
    }

    public boolean isFullyRead() {
        return unTouchedPartitions.isEmpty();
    }

    public void loadedIntoCache() {
        loadTime = System.currentTimeMillis();
    }

    public long timeSinceLoad() {
        return (System.currentTimeMillis() - loadTime) / 1000;
    }

    public long timeSinceLastTouched() {
        return (System.currentTimeMillis() - lastTouchedTime) / 1000;
    }

    public enum CacheState {
        READ_THROUGH(0), CACHABLE(1), CACHING(2), CACHED(3), EVICTED(4);

        private final byte id;

        CacheState(int id) {
            checkArgument(id < 128, "Cannot have more than 128 message types");
            this.id = (byte) id;
        }

        public byte id() {
            return id;
        }
    }

}
