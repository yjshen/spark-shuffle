package org.apache.spark.network.shuffle;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ZKClient {
    protected static final Logger LOG = LoggerFactory.getLogger(ZKClient.class);
    static int zkSessionTimeOut = 5000;
    private String hostPort;
    private ZooKeeper zkc;
    private BlockResolver handler;

    public ZKClient(String hostPort, BlockResolver handler) {
        this.hostPort = hostPort;
        this.handler = handler;
        try {
            LOG.info("Instantiate ZK Client");
            ZKConnectionWatcher zkConnectionWatcher = new ZKConnectionWatcher();
            zkc = new ZooKeeper(hostPort, zkSessionTimeOut, zkConnectionWatcher);
            zkConnectionWatcher.waitForConnection();
        } catch (IOException e) {
            throw new RuntimeException("Unable to create zk client for " + this.hostPort, e);
        }
    }

    public void watchAppDelete(String appId) {
        new AppDeleteWatcher(zkc, handler, appId);
    }

    /* Watching SyncConnected event from ZooKeeper */
    public static class ZKConnectionWatcher implements Watcher {
        private CountDownLatch clientConnectLatch = new CountDownLatch(1);

        @Override
        public void process(WatchedEvent event) {
            if (event.getState() == Event.KeeperState.SyncConnected) {
                clientConnectLatch.countDown();
            }
        }

        // Waiting for the SyncConnected event from the ZooKeeper server
        public void waitForConnection() throws IOException {
            try {
                if (!clientConnectLatch.await(zkSessionTimeOut, TimeUnit.MILLISECONDS)) {
                    throw new IOException("Couldn't connect to zookeeper server");
                }
            } catch (InterruptedException e) {
                throw new IOException("Interrupted when connecting to zookeeper server", e);
            }
        }
    }

    public static class AppDeleteWatcher implements Watcher {
        private ZooKeeper zkc;
        private BlockResolver resolver;
        private String appId;
        private String appPath;

        AppDeleteWatcher(ZooKeeper zkc, BlockResolver resolver, String appId) {
            this.zkc = zkc;
            this.resolver = resolver;
            this.appId = appId;
            this.appPath = String.format("/running-spark-apps/%s", appId);
            startWatch();
        }

        public void startWatch() {
            try {
                if (zkc.exists(appPath, false) == null) {
                    LOG.error("Failed to find app {} as a running app in ZK", appId);
                    return;
                } else {
                    zkc.exists(appPath, this);
                }
            } catch (InterruptedException e) {
                LOG.error("Exception while checking app {} existence", appId, e);
            } catch (KeeperException e) {
                LOG.error("Exception while checking app {} existence", appId, e);
            }
        }

        @Override
        public void process(WatchedEvent event) {
            switch (event.getType()) {
                case NodeDeleted:
                    resolver.applicationRemoved(appId, false);
                    break;
                default:
                    try {
                        zkc.exists(appPath, this);
                    } catch (KeeperException e) {
                        LOG.error("Exception while checking app {} existence", appId, e);
                    } catch (InterruptedException e) {
                        LOG.error("Exception while checking app {} existence", appId, e);
                    }
            }
        }
    }
}
