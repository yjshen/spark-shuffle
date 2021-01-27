package org.apache.spark.network.shuffle.cache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import com.google.common.collect.Lists;
import org.junit.Test;
import java.util.List;

public class SplitterSuite {

    void checkSplitting(
            long[] offsets,
            long totalSize,
            long targetSize,
            int[] expectedSegmentStartIndices) {

        Splitter splitter = new Splitter(offsets, totalSize, targetSize);
        splitter.splitBlocksIntoSegments();
        List<ShuffleSegment> results =
            Lists.newArrayList(splitter.getResults("", null, null).values());

        assertEquals(results.size(), expectedSegmentStartIndices.length + 1);
        long actualTotal = 0L;
        for (int i = 0; i < expectedSegmentStartIndices.length; i ++) {
            assertEquals(results.get(i).getStartPartitionId(), expectedSegmentStartIndices[i]);
            actualTotal += results.get(i).getLength();
        }
        assertTrue(actualTotal == totalSize);
    }

    @Test
    public void split1() {
        long targetSize = 100;

        // Some bytes per partition are 0 and total size is less than the target size.
        // 1 coalesced partition is expected.
        // {10, 0, 20, 0, 0}
        long[] offsets = {0, 10, 10, 30, 30};
        int[] expected = {0};
        long total = 30;
        checkSplitting(offsets, total, targetSize, expected);
    }

    @Test
    public void split2() {
        long targetSize = 100;
        // 2 coalesced partitions are expected.
        // {10, 0, 90, 21, 0}
        long[] offsets = {0, 10, 10, 100, 121};
        int[] expected = {0, 3};
        long total = 121;
        checkSplitting(offsets, total, targetSize, expected);
    }

    @Test
    public void split3() {
        long targetSize = 100;

        // There are a few large shuffle partitions.
        // {110, 10, 100, 110, 0}
        long[] offsets = {0, 110, 120, 220, 330};
        int[] expected = {0, 1, 2, 3, 4};
        long total = 331;
        checkSplitting(offsets, total, targetSize, expected);
    }

    @Test
    public void split4() {
        long targetSize = 100;

        // There are a few large shuffle partitions.
        // {10, 50, 10, 40, 20, 100, 20}
        long[] offsets = {0, 10, 60, 70, 110, 130, 230};
        int[] expected = {0, 3, 5, 6};
        long total = 250;
        checkSplitting(offsets, total, targetSize, expected);
    }

    @Test
    public void split5() {
        long targetSize = 100;

        // There are a few large shuffle partitions.
        // {10, 50, 10, 40, 100, 20}
        long[] offsets = {0, 10, 60, 70, 110, 210};
        int[] expected = {0, 4, 5};
        long total = 230;
        checkSplitting(offsets, total, targetSize, expected);
    }

    @Test
    public void split6() {
        long targetSize = 100;

        // There are a few large shuffle partitions.
        // {30, 30, 0, 40, 110}
        long[] offsets = {0, 30, 60, 60, 100};
        int[] expected = {0, 4};
        long total = 210;
        checkSplitting(offsets, total, targetSize, expected);
    }

    @Test
    public void split7() {
        long targetSize = 100;

        // merge the small partitions at the beginning/end
        // {15, 90, 15, 15, 15, 90, 15}
        long[] offsets = {0, 15, 105, 130, 145, 160, 250};
        int[] expected = {0, 2, 5};
        long total = 265;
        checkSplitting(offsets, total, targetSize, expected);
    }

    @Test
    public void split8() {
        long targetSize = 100;

        // merge the small partitions in the middle
        // {30, 15, 90, 10, 90, 15, 30}
        long[] offsets = {0, 30, 45, 135, 145, 235, 250};
        int[] expected = {0, 2, 4, 5};
        long total = 280;
        checkSplitting(offsets, total, targetSize, expected);
    }

    @Test
    public void split9() {
        long targetSize = 100;
        // merge small partitions if the combined size is smaller than
        // targetSize * MERGED_PARTITION_FACTOR
        // {35, 75, 90, 20, 35, 25, 35}
        long[] offsets = {0, 35, 110, 200, 220, 255, 280};
        int[] expected = {0, 2, 3};
        long total = 315;
        checkSplitting(offsets, total, targetSize, expected);
    }

    @Test
    public void split10() {
        long targetSize = 100;
        // merge small partitions if the combined size is smaller than
        // targetSize * MERGED_PARTITION_FACTOR
        // {0, 0, 0, 110, 10, 25, 35}
        long[] offsets = {0, 0, 0, 0, 110, 120, 145};
        int[] expected = {3, 4};
        long total = 180;
        checkSplitting(offsets, total, targetSize, expected);
    }

    @Test
    public void split11() {
        long targetSize = 100;
        // merge small partitions if the combined size is smaller than
        // targetSize * MERGED_PARTITION_FACTOR
        // {0, 0, 0, 90, 10, 25, 35}
        long[] offsets = {0, 0, 0, 0, 90, 100, 125};
        int[] expected = {0, 5};
        long total = 160;
        checkSplitting(offsets, total, targetSize, expected);
    }

    @Test
    public void split12() {
        long targetSize = 100;
        // merge small partitions if the combined size is smaller than
        // targetSize * MERGED_PARTITION_FACTOR
        // {0, 0, 0, 110, 0, 0, 0, 110, 0, 0, 0, 10, 25, 35}
        long[] offsets = {0, 0, 0, 0, 110, 110, 110, 110, 220, 220, 220, 220, 230, 255};
        int[] expected = {3, 7, 8};
        long total = 290;
        checkSplitting(offsets, total, targetSize, expected);
    }
}