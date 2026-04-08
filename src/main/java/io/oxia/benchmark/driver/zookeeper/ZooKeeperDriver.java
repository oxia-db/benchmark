/*
 * Copyright © 2025 The Oxia Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.oxia.benchmark.driver.zookeeper;

import io.oxia.benchmark.driver.KVStoreDriver;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import lombok.CustomLog;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

@CustomLog
public class ZooKeeperDriver implements KVStoreDriver {

    private ZooKeeper zk;

    @Override
    public String name() {
        return "zookeeper";
    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(Map<String, Object> config) throws Exception {
        log.info().attr("config", config).log("Initializing ZooKeeper driver");

        Object serversObj = config.get("servers");
        if (serversObj == null) {
            throw new IllegalArgumentException("Missing 'servers' in ZooKeeper config");
        }

        List<String> servers;
        if (serversObj instanceof List<?> list) {
            servers = list.stream().map(Object::toString).toList();
        } else {
            throw new IllegalArgumentException("Invalid type for 'servers': " + serversObj.getClass());
        }

        String connectString = String.join(",", servers);
        CountDownLatch connectedLatch = new CountDownLatch(1);

        zk =
                new ZooKeeper(
                        connectString,
                        30_000,
                        event -> {
                            if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                                connectedLatch.countDown();
                            }
                            log.info().attr("event", event).log("ZK event");
                        });

        if (!connectedLatch.await(30, TimeUnit.SECONDS)) {
            throw new IOException("Timed out waiting for ZooKeeper connection");
        }

        // Ensure base node exists
        try {
            zk.create("/test", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException e) {
            // OK, already exists
        }
    }

    @Override
    public CompletableFuture<Void> put(String key, byte[] value) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        String path = "/test/" + key;

        // Try to set first (most common case for existing keys)
        zk.setData(
                path,
                value,
                -1,
                (rc, p, ctx, stat) -> {
                    if (rc == KeeperException.Code.OK.intValue()) {
                        future.complete(null);
                    } else if (rc == KeeperException.Code.NONODE.intValue()) {
                        // Node doesn't exist, try to create it
                        zk.create(
                                path,
                                value,
                                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                                CreateMode.PERSISTENT,
                                (rc2, p2, ctx2, name) -> {
                                    if (rc2 == KeeperException.Code.OK.intValue()) {
                                        future.complete(null);
                                    } else if (rc2 == KeeperException.Code.NODEEXISTS.intValue()) {
                                        // Another thread created it; retry set
                                        zk.setData(
                                                path,
                                                value,
                                                -1,
                                                (rc3, p3, ctx3, stat3) -> {
                                                    if (rc3 == KeeperException.Code.OK.intValue()) {
                                                        future.complete(null);
                                                    } else {
                                                        future.completeExceptionally(
                                                                KeeperException.create(KeeperException.Code.get(rc3), path));
                                                    }
                                                },
                                                null);
                                    } else {
                                        future.completeExceptionally(
                                                KeeperException.create(KeeperException.Code.get(rc2), path));
                                    }
                                },
                                null);
                    } else {
                        future.completeExceptionally(
                                KeeperException.create(KeeperException.Code.get(rc), path));
                    }
                },
                null);

        return future;
    }

    @Override
    public CompletableFuture<Void> get(String key) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        String path = "/test/" + key;

        zk.getData(
                path,
                false,
                (rc, p, ctx, data, stat) -> {
                    if (rc == KeeperException.Code.OK.intValue()
                            || rc == KeeperException.Code.NONODE.intValue()) {
                        future.complete(null);
                    } else {
                        future.completeExceptionally(
                                KeeperException.create(KeeperException.Code.get(rc), path));
                    }
                },
                null);

        return future;
    }

    @Override
    public void close() throws IOException {
        if (zk != null) {
            try {
                zk.close();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException(e);
            }
        }
    }
}
