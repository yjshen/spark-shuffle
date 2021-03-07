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

import static org.apache.spark.network.util.NettyUtils.getRemoteAddress;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricSet;
import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.server.OneForOneStreamManager;
import org.apache.spark.network.server.RpcHandler;
import org.apache.spark.network.server.StreamManager;
import org.apache.spark.network.shuffle.protocol.BlockTransferMessage;
import org.apache.spark.network.shuffle.protocol.BlocksRemoved;
import org.apache.spark.network.shuffle.protocol.FetchShuffleBlocks;
import org.apache.spark.network.shuffle.protocol.GetLocalDirsForExecutors;
import org.apache.spark.network.shuffle.protocol.LocalDirsForExecutors;
import org.apache.spark.network.shuffle.protocol.OpenBlocks;
import org.apache.spark.network.shuffle.protocol.RegisterExecutor;
import org.apache.spark.network.shuffle.protocol.RemoveBlocks;
import org.apache.spark.network.shuffle.protocol.StreamHandle;
import org.apache.spark.network.util.TransportConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * RPC Handler for a server which can serve shuffle blocks from outside of an Executor process.
 * <p>
 * Handles registering executors and opening shuffle blocks from them. Shuffle blocks are registered
 * with the "one-for-one" strategy, meaning each Transport-layer Chunk is equivalent to one Spark-
 * level shuffle block.
 */
public class ExternalShuffleBlockHandlerWithCache extends RpcHandler {
    private static final Logger logger =
        LoggerFactory.getLogger(ExternalShuffleBlockHandlerWithCache.class);

    @VisibleForTesting
    final ExternalShuffleBlockResolverWithCache blockManager;
    private final OneForOneStreamManager streamManager;
    private final ShuffleMetrics metrics;
    private final MetricRegistry registry;

    public ExternalShuffleBlockHandlerWithCache(
        TransportConf conf,
        MetricRegistry registry)
        throws IOException {
        this(
            new OneForOneStreamManager(),
            new ExternalShuffleBlockResolverWithCache(conf, registry),
            registry);
    }

    /**
     * Enables mocking out the StreamManager and BlockManager.
     */
    @VisibleForTesting
    public ExternalShuffleBlockHandlerWithCache(
        OneForOneStreamManager streamManager,
        ExternalShuffleBlockResolverWithCache blockManager,
        MetricRegistry registry) {
        this.metrics = new ShuffleMetrics();
        this.registry = registry;
        this.registry.registerAll(metrics);
        this.streamManager = streamManager;
        this.blockManager = blockManager;
    }

    MetricRegistry getRegistry() {
        return registry;
    }

    @Override
    public void receive(TransportClient client, ByteBuffer message, RpcResponseCallback callback) {
        BlockTransferMessage msgObj = BlockTransferMessage.Decoder.fromByteBuffer(message);
        handleMessage(msgObj, client, callback);
    }

    protected void handleMessage(
        BlockTransferMessage msgObj,
        TransportClient client,
        RpcResponseCallback callback) {
        if (msgObj instanceof OpenBlocks) {
            final Timer.Context responseDelayContext = metrics.openBlockRequestLatencyMillis.time();
            try {
                OpenBlocks msg = (OpenBlocks) msgObj;
                checkAuth(client, msg.appId);
                long streamId = streamManager.registerStream(client.getClientId(),
                    new ManagedBufferIterator(msg.appId, msg.execId, msg.blockIds), client.getChannel());
                if (logger.isTraceEnabled()) {
                    logger.trace("Registered streamId {} with {} buffers for client {} from host {}",
                        streamId,
                        msg.blockIds.length,
                        client.getClientId(),
                        getRemoteAddress(client.getChannel()));
                }
                callback.onSuccess(new StreamHandle(streamId, msg.blockIds.length).toByteBuffer());
            } finally {
                responseDelayContext.stop();
            }

        } else if (msgObj instanceof FetchShuffleBlocks) {
            final Timer.Context responseDelayContext = metrics.openBlockRequestLatencyMillis.time();
            try {
                int numBlockIds;
                long streamId;
                FetchShuffleBlocks msg = (FetchShuffleBlocks) msgObj;
                checkAuth(client, msg.appId);
                numBlockIds = 0;
                if (msg.batchFetchEnabled) {
                    numBlockIds = msg.mapIds.length;
                } else {
                    for (int[] ids: msg.reduceIds) {
                        numBlockIds += ids.length;
                    }
                }
                streamId = streamManager.registerStream(client.getClientId(),
                    new ShuffleManagedBufferIterator(msg), client.getChannel());
                if (logger.isTraceEnabled()) {
                    logger.trace(
                        "Registered streamId {} with {} buffers for client {} from host {}",
                        streamId,
                        numBlockIds,
                        client.getClientId(),
                        getRemoteAddress(client.getChannel()));
                }
                callback.onSuccess(new StreamHandle(streamId, numBlockIds).toByteBuffer());
            } finally {
                responseDelayContext.stop();
            }

        } else if (msgObj instanceof RegisterExecutor) {
            final Timer.Context responseDelayContext =
                metrics.registerExecutorRequestLatencyMillis.time();
            try {
                RegisterExecutor msg = (RegisterExecutor) msgObj;
                checkAuth(client, msg.appId);
                blockManager.registerExecutor(msg.appId, msg.execId, msg.executorInfo);
                callback.onSuccess(ByteBuffer.wrap(new byte[0]));
            } finally {
                responseDelayContext.stop();
            }

        } else if (msgObj instanceof RemoveBlocks) {
            RemoveBlocks msg = (RemoveBlocks) msgObj;
            checkAuth(client, msg.appId);
            int numRemovedBlocks = blockManager.removeBlocks(msg.appId, msg.execId, msg.blockIds);
            callback.onSuccess(new BlocksRemoved(numRemovedBlocks).toByteBuffer());

        } else if (msgObj instanceof GetLocalDirsForExecutors) {
            GetLocalDirsForExecutors msg = (GetLocalDirsForExecutors) msgObj;
            checkAuth(client, msg.appId);
            Map<String, String[]> localDirs = blockManager.getLocalDirs(msg.appId, msg.execIds);
            callback.onSuccess(new LocalDirsForExecutors(localDirs).toByteBuffer());

        } else {
            throw new UnsupportedOperationException("Unexpected message: " + msgObj);
        }
    }

    @Override
    public StreamManager getStreamManager() {
        return streamManager;
    }

    /**
     * Removes an application (once it has been terminated), and optionally will clean up any
     * local directories associated with the executors of that application in a separate thread.
     */
    public void applicationRemoved(String appId, boolean cleanupLocalDirs) {
        blockManager.applicationRemoved(appId, cleanupLocalDirs);
    }

    public void close() {
        blockManager.close();
    }

    private void checkAuth(TransportClient client, String appId) {
        if (client.getClientId() != null && !client.getClientId().equals(appId)) {
            throw new SecurityException(String.format(
                "Client for %s not authorized for application %s.", client.getClientId(), appId));
        }
    }

    private boolean isShuffleBlock(String[] blockIdParts) {
        // length == 4: ShuffleBlockId
        // length == 5: ContinuousShuffleBlockId
        return (blockIdParts.length == 4 || blockIdParts.length == 5) &&
            blockIdParts[0].equals("shuffle");
    }

    /**
     * A simple class to wrap all shuffle service wrapper metrics
     */
    private class ShuffleMetrics implements MetricSet {
        private final Map<String, Metric> allMetrics;
        // Time latency for open block request in ms
        private final Timer openBlockRequestLatencyMillis = new Timer();
        // Time latency for executor registration latency in ms
        private final Timer registerExecutorRequestLatencyMillis = new Timer();
        // Block transfer rate in byte per second
        private final Meter blockTransferRateBytes = new Meter();
        // Number of active connections to the shuffle service
        private Counter activeConnections = new Counter();
        // Number of exceptions caught in connections to the shuffle service
        private Counter caughtExceptions = new Counter();

        private ShuffleMetrics() {
            allMetrics = new HashMap<>();
            allMetrics.put("openBlockRequestLatencyMillis", openBlockRequestLatencyMillis);
            allMetrics.put("registerExecutorRequestLatencyMillis", registerExecutorRequestLatencyMillis);
            allMetrics.put("blockTransferRateBytes", blockTransferRateBytes);
            allMetrics.put("registeredExecutorsSize",
                (Gauge<Integer>) () -> blockManager.getRegisteredExecutorsSize());
            allMetrics.put("numActiveConnections", activeConnections);
            allMetrics.put("numCaughtExceptions", caughtExceptions);
        }

        @Override
        public Map<String, Metric> getMetrics() {
            return allMetrics;
        }
    }

    private class ManagedBufferIterator implements Iterator<ManagedBuffer> {

        private final String appId;
        private final String execId;
        private final int shuffleId;
        // An array containing mapId, reduceId and numBlocks tuple
        private final int[] shuffleBlockIds;
        private int index = 0;

        ManagedBufferIterator(String appId, String execId, String[] blockIds) {
            this.appId = appId;
            this.execId = execId;
            String[] blockId0Parts = blockIds[0].split("_");
            if (!isShuffleBlock(blockId0Parts)) {
                throw new IllegalArgumentException("Unexpected shuffle block id format: " + blockIds[0]);
            }
            this.shuffleId = Integer.parseInt(blockId0Parts[1]);
            shuffleBlockIds = new int[3 * blockIds.length];
            for (int i = 0; i < blockIds.length; i++) {
                String[] blockIdParts = blockIds[i].split("_");
                if (!isShuffleBlock(blockIdParts)) {
                    throw new IllegalArgumentException("Unexpected shuffle block id format: " + blockIds[i]);
                }
                if (Integer.parseInt(blockIdParts[1]) != shuffleId) {
                    throw new IllegalArgumentException("Expected shuffleId=" + shuffleId +
                        ", got:" + blockIds[i]);
                }
                shuffleBlockIds[3 * i] = Integer.parseInt(blockIdParts[2]);
                shuffleBlockIds[3 * i + 1] = Integer.parseInt(blockIdParts[3]);
                if (blockIdParts.length == 4) {
                    shuffleBlockIds[3 * i + 2] = 1;
                } else {
                    shuffleBlockIds[3 * i + 2] = Integer.parseInt(blockIdParts[4]);
                }
            }
        }

        @Override
        public boolean hasNext() {
            return index < shuffleBlockIds.length;
        }

        @Override
        public ManagedBuffer next() {
            final ManagedBuffer block = blockManager.getBlockData(appId, execId, shuffleId,
                shuffleBlockIds[index],
                shuffleBlockIds[index + 1],
                shuffleBlockIds[index + 2]);
            index += 3;
            metrics.blockTransferRateBytes.mark(block != null ? block.size() : 0);
            return block;
        }
    }

    private class ShuffleManagedBufferIterator implements Iterator<ManagedBuffer> {

        private int mapIdx = 0;
        private int reduceIdx = 0;

        private final String appId;
        private final String execId;
        private final int shuffleId;
        private final long[] mapIds;
        private final int[][] reduceIds;
        private final boolean batchFetchEnabled;

        ShuffleManagedBufferIterator(FetchShuffleBlocks msg) {
            appId = msg.appId;
            execId = msg.execId;
            shuffleId = msg.shuffleId;
            mapIds = msg.mapIds;
            reduceIds = msg.reduceIds;
            batchFetchEnabled = msg.batchFetchEnabled;
        }

        @Override
        public boolean hasNext() {
            // mapIds.length must equal to reduceIds.length, and the passed in FetchShuffleBlocks
            // must have non-empty mapIds and reduceIds, see the checking logic in
            // OneForOneBlockFetcher.
            assert(mapIds.length != 0 && mapIds.length == reduceIds.length);
            return mapIdx < mapIds.length && reduceIdx < reduceIds[mapIdx].length;
        }

        @Override
        public ManagedBuffer next() {
            ManagedBuffer block;
            if (!batchFetchEnabled) {
                block = blockManager.getBlockData(
                    appId, execId, shuffleId, mapIds[mapIdx], reduceIds[mapIdx][reduceIdx], 1);
                if (reduceIdx < reduceIds[mapIdx].length - 1) {
                    reduceIdx += 1;
                } else {
                    reduceIdx = 0;
                    mapIdx += 1;
                }
            } else {
                assert(reduceIds[mapIdx].length == 2);
                block = blockManager.getBlockData(appId, execId, shuffleId, mapIds[mapIdx],
                    reduceIds[mapIdx][0], reduceIds[mapIdx][1] - reduceIds[mapIdx][0]);
                mapIdx += 1;
            }
            metrics.blockTransferRateBytes.mark(block != null ? block.size() : 0);
            return block;
        }
    }

    @Override
    public void channelActive(TransportClient client) {
        metrics.activeConnections.inc();
        super.channelActive(client);
    }

    @Override
    public void channelInactive(TransportClient client) {
        metrics.activeConnections.dec();
        super.channelInactive(client);
    }
}
