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

import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;

/**
 * The reply to remove blocks giving back the number of removed blocks.
 */
public class BlocksRemoved extends BlockTransferMessage {
    public final int numRemovedBlocks;

    public BlocksRemoved(int numRemovedBlocks) {
        this.numRemovedBlocks = numRemovedBlocks;
    }

    public static BlocksRemoved decode(ByteBuf buf) {
        int numRemovedBlocks = buf.readInt();
        return new BlocksRemoved(numRemovedBlocks);
    }

    @Override
    protected Type type() {
        return Type.BLOCKS_REMOVED;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(numRemovedBlocks);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
            .add("numRemovedBlocks", numRemovedBlocks)
            .toString();
    }

    @Override
    public boolean equals(Object other) {
        if (other != null && other instanceof BlocksRemoved) {
            BlocksRemoved o = (BlocksRemoved) other;
            return Objects.equal(numRemovedBlocks, o.numRemovedBlocks);
        }
        return false;
    }

    @Override
    public int encodedLength() {
        return 4;
    }

    @Override
    public void encode(ByteBuf buf) {
        buf.writeInt(numRemovedBlocks);
    }
}
