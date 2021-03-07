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

import com.carrotsearch.hppc.IntHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.util.ArrayList;
import java.util.NavigableMap;
import java.util.TreeMap;

public class Splitter {
    private static final Logger logger = LoggerFactory.getLogger(Splitter.class);

    private final double MERGED_PARTITION_FACTOR = 1.2;

    private final long[] offsets;
    private final long totalSize;
    private final long targetSize;
    private final long[] sizes;

    ArrayList<Integer> segmentStartIndices = new ArrayList<>();
    int i = 0;
    long currentSegmentSize = 0L;
    long lastSegmentSize = 0L;
    int partitionInLastSegment = 0;
    int partitionInCurrentSegment = 0;

    public Splitter(long[] offsets, long totalSize, long targetSize) {
        this.offsets = offsets;
        this.totalSize = totalSize;
        this.targetSize = targetSize;
        this.sizes = new long[offsets.length];
        for (int i = 0; i < offsets.length - 1; i++) {
            sizes[i] = offsets[i + 1] - offsets[i];
        }
        sizes[sizes.length - 1] = totalSize - offsets[sizes.length - 1];
        segmentStartIndices.add(0);

        StringIntMap
    }

    /**
     * The size sum of each shuffle segment is close to the target size.
     */
    public void splitBlocksIntoSegments() {
        while (i < sizes.length) {
            // If including the next size in the current partition exceeds the target size, package the
            // current partition and start a new partition.
            if (i > 0 && currentSegmentSize + sizes[i] > targetSize) {
                tryMergeBlocks();
                segmentStartIndices.add(i);
                currentSegmentSize = sizes[i];
                partitionInCurrentSegment = 1;
            } else {
                currentSegmentSize += sizes[i];
                partitionInCurrentSegment += 1;
            }
            i += 1;
        }
        tryMergeBlocks();
    }

    void tryMergeBlocks() {
        // When we are going to start a new segment, it's possible that the current block or
        // the previous block is very small and it's better to merge the current block into
        // the previous segment.
        boolean cannotMerge =
            lastSegmentSize >= targetSize && partitionInLastSegment == 1
                || currentSegmentSize >= targetSize && partitionInCurrentSegment == 1
                || lastSegmentSize + currentSegmentSize > targetSize * MERGED_PARTITION_FACTOR;

        if (cannotMerge) {
            lastSegmentSize = currentSegmentSize;
            partitionInLastSegment = partitionInCurrentSegment;
        } else {
            // We decide to merge the current block into the previous one, so the start index of
            // the current block should be removed.
            if (segmentStartIndices.size() != 1) {
                segmentStartIndices.remove(segmentStartIndices.size() - 1);
            }
            lastSegmentSize += currentSegmentSize;
            partitionInLastSegment += partitionInCurrentSegment;
        }
    }

    public NavigableMap<Long, ShuffleSegment> getResults(
        String appId, File indexFile, File dataFile) {
        NavigableMap<Long, ShuffleSegment> splitResults = new TreeMap<>();
        int outputSegmentsNum = segmentStartIndices.size();
        for (int i = 0; i < outputSegmentsNum; i++) {
            int startPartitionId = segmentStartIndices.get(i);
            long startOffset = offsets[startPartitionId];
            long length;
            int numPartitions;
            if (i == outputSegmentsNum - 1) {
                length = totalSize - startOffset;
                numPartitions = offsets.length - startPartitionId;
            } else {
                int nextStartIndex = segmentStartIndices.get(i + 1);
                length = offsets[nextStartIndex] - startOffset;
                numPartitions = nextStartIndex - startPartitionId;
            }
            if (length > 0) {
                IntHashSet nonEmptyPartitions = getNonEmptyPartitions(startPartitionId, numPartitions);
                ShuffleSegment current = new ShuffleSegment(
                    appId, indexFile, dataFile, startPartitionId, numPartitions, startOffset, length, nonEmptyPartitions);
                splitResults.put(startOffset, current);
            }
        }
        // Append a dummy segment starts with Long.Max for easier segments finding
        splitResults.put(Long.MAX_VALUE, ShuffleSegment.DUMMY_SEGMENT);
        return splitResults;
    }

    public IntHashSet getNonEmptyPartitions(int startPartitionId, int numPartitions) {
        int numNonEmptyPartitions = 0;
        for (int i = 0; i < numPartitions; i++) {
            if (sizes[i + startPartitionId] != 0) {
                numNonEmptyPartitions += 1;
            }
        }
        IntHashSet set = new IntHashSet(numNonEmptyPartitions);
        for (int i = 0; i < numPartitions; i++) {
            if (sizes[i + startPartitionId] != 0) {
                set.add(i + startPartitionId);
            }
        }
        return set;
    }
}
