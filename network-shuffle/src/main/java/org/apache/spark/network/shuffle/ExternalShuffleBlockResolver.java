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

package org.apache.spark.network.shuffle;

import com.google.common.annotations.VisibleForTesting;
import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.shuffle.protocol.ExecutorShuffleInfo;
import org.apache.spark.network.util.NettyUtils;
import org.apache.spark.network.util.TransportConf;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * Manages converting shuffle BlockIds into physical segments of local files, from a process outside
 * of Executors. Each Executor must register its own configuration about where it stores its files
 * (local dirs) and how (shuffle manager). The logic for retrieval of individual files is replicated
 * from Spark's IndexShuffleBlockResolver.
 */
public class ExternalShuffleBlockResolver extends BlockResolver {

    public ExternalShuffleBlockResolver(TransportConf conf) {
        this(conf, Executors.newSingleThreadExecutor(
            // Add `spark` prefix because it will run in NM in Yarn mode.
            NettyUtils.createThreadFactory("spark-shuffle-directory-cleaner")));
    }

    // Allows tests to have more control over when directories are cleaned up.
    @VisibleForTesting
    ExternalShuffleBlockResolver(TransportConf conf, Executor directoryCleaner) {
        super(conf, directoryCleaner);
    }

    /**
     * Obtains a FileSegmentManagedBuffer from (shuffleId, mapId, reduceId, numBlocks). We make assumptions
     * about how the hash and sort based shuffles store their data.
     */
    public ManagedBuffer getBlockData(
        String appId,
        String execId,
        int shuffleId,
        int mapId,
        int reduceId,
        int numBlocks) {
        ExecutorShuffleInfo executor = executors.get(new AppExecId(appId, execId));
        if (executor == null) {
            throw new RuntimeException(
                String.format("Executor is not registered (appId=%s, execId=%s)", appId, execId));
        }
        return getSortBasedShuffleBlockData(executor, shuffleId, mapId, reduceId, numBlocks);
    }

    public ManagedBuffer getRddBlockData(
        String appId,
        String execId,
        int rddId,
        int splitIndex) {
        ExecutorShuffleInfo executor = executors.get(new AppExecId(appId, execId));
        if (executor == null) {
            throw new RuntimeException(
                String.format("Executor is not registered (appId=%s, execId=%s)", appId, execId));
        }
        return getDiskPersistedRddBlockData(executor, rddId, splitIndex);
    }
}
