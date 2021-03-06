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

import com.codahale.metrics.MetricSet;
import com.google.common.collect.Lists;
import org.apache.spark.network.TransportContext;
import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.client.TransportClientBootstrap;
import org.apache.spark.network.client.TransportClientFactory;
import org.apache.spark.network.server.NoOpRpcHandler;
import org.apache.spark.network.shuffle.protocol.BlockTransferMessage;
import org.apache.spark.network.shuffle.protocol.BlocksRemoved;
import org.apache.spark.network.shuffle.protocol.ExecutorShuffleInfo;
import org.apache.spark.network.shuffle.protocol.RegisterExecutor;
import org.apache.spark.network.shuffle.protocol.RemoveBlocks;
import org.apache.spark.network.util.TransportConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 * Client for reading shuffle blocks which points to an external (outside of executor) server.
 * This is instead of reading shuffle blocks directly from other executors (via
 * BlockTransferService), which has the downside of losing the shuffle data if we lose the
 * executors.
 */
public class ExternalShuffleClient extends ShuffleClient {
    private static final Logger logger = LoggerFactory.getLogger(ExternalShuffleClient.class);

    private final TransportConf conf;
    private final long registrationTimeoutMs;

    protected volatile TransportClientFactory clientFactory;
    protected String appId;

    /**
     * Creates an external shuffle client, with SASL optionally enabled. If SASL is not enabled,
     * then secretKeyHolder may be null.
     */
    public ExternalShuffleClient(
        TransportConf conf,
        long registrationTimeoutMs) {
        this.conf = conf;
        this.registrationTimeoutMs = registrationTimeoutMs;
    }

    protected void checkInit() {
        assert appId != null : "Called before init()";
    }

    /**
     * Initializes the ShuffleClient, specifying this Executor's appId.
     * Must be called before any other method on the ShuffleClient.
     */
    public void init(String appId) {
        this.appId = appId;
        TransportContext context = new TransportContext(conf, new NoOpRpcHandler(), true);
        List<TransportClientBootstrap> bootstraps = Lists.newArrayList();
        clientFactory = context.createClientFactory(bootstraps);
    }

    @Override
    public void fetchBlocks(
        String host,
        int port,
        String execId,
        String[] blockIds,
        BlockFetchingListener listener,
        DownloadFileManager downloadFileManager) {
        checkInit();
        logger.debug("External shuffle fetch from {}:{} (executor id {})", host, port, execId);
        try {
            RetryingBlockFetcher.BlockFetchStarter blockFetchStarter =
                (blockIds1, listener1) -> {
                    // Unless this client is closed.
                    if (clientFactory != null) {
                        TransportClient client = clientFactory.createClient(host, port);
                        new OneForOneBlockFetcher(client, appId, execId,
                            blockIds1, listener1, conf, downloadFileManager).startForDebug();
                    } else {
                        logger.info("This clientFactory was closed. Skipping further block fetch retries.");
                    }
                };

            int maxRetries = conf.maxIORetries();
            if (maxRetries > 0) {
                // Note this Fetcher will correctly handle maxRetries == 0; we avoid it just in case there's
                // a bug in this code. We should remove the if statement once we're sure of the stability.
                new RetryingBlockFetcher(conf, blockFetchStarter, blockIds, listener).start();
            } else {
                blockFetchStarter.createAndStart(blockIds, listener);
            }
        } catch (Exception e) {
            logger.error("Exception while beginning fetchBlocks", e);
            for (String blockId : blockIds) {
                listener.onBlockFetchFailure(blockId, e);
            }
        }
    }

    @Override
    public MetricSet shuffleMetrics() {
        checkInit();
        return clientFactory.getAllMetrics();
    }

    /**
     * Registers this executor with an external shuffle server. This registration is required to
     * inform the shuffle server about where and how we store our shuffle files.
     *
     * @param host         Host of shuffle server.
     * @param port         Port of shuffle server.
     * @param execId       This Executor's id.
     * @param executorInfo Contains all info necessary for the service to find our shuffle files.
     */
    public void registerWithShuffleServer(
        String host,
        int port,
        String execId,
        ExecutorShuffleInfo executorInfo) throws IOException, InterruptedException {
        checkInit();
        try (TransportClient client = clientFactory.createClient(host, port)) {
            ByteBuffer registerMessage = new RegisterExecutor(appId, execId, executorInfo).toByteBuffer();
            client.sendRpcSync(registerMessage, registrationTimeoutMs);
        }
    }

    public Future<Integer> removeBlocks(
        String host,
        int port,
        String execId,
        String[] blockIds) throws IOException, InterruptedException {
        checkInit();
        CompletableFuture<Integer> numRemovedBlocksFuture = new CompletableFuture<>();
        ByteBuffer removeBlocksMessage = new RemoveBlocks(appId, execId, blockIds).toByteBuffer();
        final TransportClient client = clientFactory.createClient(host, port);
        client.sendRpc(removeBlocksMessage, new RpcResponseCallback() {
            @Override
            public void onSuccess(ByteBuffer response) {
                BlockTransferMessage msgObj = BlockTransferMessage.Decoder.fromByteBuffer(response);
                numRemovedBlocksFuture.complete(((BlocksRemoved) msgObj).numRemovedBlocks);
                client.close();
            }

            @Override
            public void onFailure(Throwable e) {
                logger.warn("Error trying to remove RDD blocks " + Arrays.toString(blockIds) +
                    " via external shuffle service from executor: " + execId, e);
                numRemovedBlocksFuture.complete(0);
                client.close();
            }
        });
        return numRemovedBlocksFuture;
    }

    @Override
    public void close() {
        checkInit();
        if (clientFactory != null) {
            clientFactory.close();
            clientFactory = null;
        }
    }
}
