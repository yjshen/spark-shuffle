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

package org.apache.spark.network.shuffle.protocol;

import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.spark.network.protocol.Encoders;
import java.util.Arrays;

// Needed by ScalaDoc. See SPARK-7726


/** Request to read a set of blocks. Returns {@link StreamHandle}. */
public class FetchShuffleBlocks extends BlockTransferMessage {
  public final String appId;
  public final String execId;
  public final int shuffleId;
  // The length of mapIds must equal to reduceIds.size(), for the i-th mapId in mapIds,
  // it corresponds to the i-th int[] in reduceIds, which contains all reduce id for this map id.
  public final long[] mapIds;
  // When batchFetchEnabled=true, reduceIds[i] contains 2 elements: startReduceId (inclusive) and
  // endReduceId (exclusive) for the mapper mapIds[i].
  // When batchFetchEnabled=false, reduceIds[i] contains all the reduce IDs that mapper mapIds[i]
  // needs to fetch.
  public final int[][] reduceIds;
  public final boolean batchFetchEnabled;

  public FetchShuffleBlocks(
      String appId,
      String execId,
      int shuffleId,
      long[] mapIds,
      int[][] reduceIds,
      boolean batchFetchEnabled) {
    this.appId = appId;
    this.execId = execId;
    this.shuffleId = shuffleId;
    this.mapIds = mapIds;
    this.reduceIds = reduceIds;
    assert(mapIds.length == reduceIds.length);
    this.batchFetchEnabled = batchFetchEnabled;
    if (batchFetchEnabled) {
      for (int[] ids: reduceIds) {
        assert(ids.length == 2);
      }
    }
  }

  @Override
  protected Type type() { return Type.FETCH_SHUFFLE_BLOCKS; }

  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
      .append("appId", appId)
      .append("execId", execId)
      .append("shuffleId", shuffleId)
      .append("mapIds", Arrays.toString(mapIds))
      .append("reduceIds", Arrays.deepToString(reduceIds))
      .append("batchFetchEnabled", batchFetchEnabled)
      .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    FetchShuffleBlocks that = (FetchShuffleBlocks) o;

    if (shuffleId != that.shuffleId) return false;
    if (batchFetchEnabled != that.batchFetchEnabled) return false;
    if (!appId.equals(that.appId)) return false;
    if (!execId.equals(that.execId)) return false;
    if (!Arrays.equals(mapIds, that.mapIds)) return false;
    return Arrays.deepEquals(reduceIds, that.reduceIds);
  }

  @Override
  public int hashCode() {
    int result = appId.hashCode();
    result = 31 * result + execId.hashCode();
    result = 31 * result + shuffleId;
    result = 31 * result + Arrays.hashCode(mapIds);
    result = 31 * result + Arrays.deepHashCode(reduceIds);
    result = 31 * result + (batchFetchEnabled ? 1 : 0);
    return result;
  }

  @Override
  public int encodedLength() {
    int encodedLengthOfReduceIds = 0;
    for (int[] ids: reduceIds) {
      encodedLengthOfReduceIds += Encoders.IntArrays.encodedLength(ids);
    }
    return Encoders.Strings.encodedLength(appId)
      + Encoders.Strings.encodedLength(execId)
      + 4 /* encoded length of shuffleId */
      + Encoders.LongArrays.encodedLength(mapIds)
      + 4 /* encoded length of reduceIds.size() */
      + encodedLengthOfReduceIds
      + 1; /* encoded length of batchFetchEnabled */
  }

  @Override
  public void encode(ByteBuf buf) {
    Encoders.Strings.encode(buf, appId);
    Encoders.Strings.encode(buf, execId);
    buf.writeInt(shuffleId);
    Encoders.LongArrays.encode(buf, mapIds);
    buf.writeInt(reduceIds.length);
    for (int[] ids: reduceIds) {
      Encoders.IntArrays.encode(buf, ids);
    }
    buf.writeBoolean(batchFetchEnabled);
  }

  public static FetchShuffleBlocks decode(ByteBuf buf) {
    String appId = Encoders.Strings.decode(buf);
    String execId = Encoders.Strings.decode(buf);
    int shuffleId = buf.readInt();
    long[] mapIds = Encoders.LongArrays.decode(buf);
    int reduceIdsSize = buf.readInt();
    int[][] reduceIds = new int[reduceIdsSize][];
    for (int i = 0; i < reduceIdsSize; i++) {
      reduceIds[i] = Encoders.IntArrays.decode(buf);
    }
    boolean batchFetchEnabled = buf.readBoolean();
    return new FetchShuffleBlocks(appId, execId, shuffleId, mapIds, reduceIds, batchFetchEnabled);
  }
}
