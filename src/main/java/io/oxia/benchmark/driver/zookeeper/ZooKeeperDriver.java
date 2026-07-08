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

import io.oxia.benchmark.driver.session.SessionDriver;
import io.oxia.benchmark.driver.session.SessionHandle;
import java.io.IOException;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import lombok.CustomLog;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

/**
 * ZooKeeper driver, serving both the KV workloads (put/get on persistent znodes under {@code
 * /test}) and the session experiments. ZK's model <em>is</em> one session per client / TCP
 * connection, so we keep it: each benchmark session is its own {@link ZooKeeper} handle, and
 * ephemeral keys are ephemeral znodes created on that handle. The SendThread pings the server at
 * ~timeout/3 automatically, so the heartbeat cadence matches the other systems by construction. The
 * negotiated timeout is bounded by the server's {@code tickTime} (min 2·tick, max 20·tick);
 * configure {@code tickTime} so the chosen timeout is admissible (10s is fine with the default 2s
 * tick).
 *
 * <ul>
 *   <li><b>Graceful close</b> — {@code ZooKeeper.close()} sends the close-session request; the
 *       server drops the session and its ephemerals at once.
 *   <li><b>Abrupt kill</b> — {@code ClientCnxn.disconnect()} (reached reflectively) stops the
 *       SendThread and closes the socket <em>without</em> the close-session request, so the server
 *       only reaps the session via the expiry path. Calling {@code close()} would instead end the
 *       session gracefully and skip that path, which is exactly what we must avoid.
 * </ul>
 */
@CustomLog
public class ZooKeeperDriver implements SessionDriver {

    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(30);
    private static final String KV_ROOT = "/test";

    private String connectString;
    private ZooKeeper zk; // the main connection: KV workload + session-experiment foreground load
    private ExecutorService
            blockingIo; // ZK connect/close/disconnect block; keep them off pool threads

    @Override
    public String name() {
        return "zookeeper";
    }

    @Override
    public void init(Map<String, Object> config) throws Exception {
        log.info().attr("config", config).log("Initializing ZooKeeper driver");

        Object serversObj = config.get("servers");
        if (!(serversObj instanceof List<?> list)) {
            throw new IllegalArgumentException("Missing/invalid 'servers' in ZooKeeper config");
        }
        connectString = String.join(",", list.stream().map(Object::toString).toList());
        blockingIo =
                Executors.newCachedThreadPool(
                        r -> {
                            Thread t = new Thread(r, "zk-session-io");
                            t.setDaemon(true);
                            return t;
                        });

        zk = connect(connectString, CONNECT_TIMEOUT);
        // Ensure the KV base node exists
        createPersistentIfAbsent(zk, KV_ROOT).get(CONNECT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

    /** Open a ZooKeeper connection and block only the caller until SyncConnected. */
    private static ZooKeeper connect(String connectString, Duration timeout) throws Exception {
        CountDownLatch connected = new CountDownLatch(1);
        ZooKeeper zk =
                new ZooKeeper(
                        connectString,
                        (int) timeout.toMillis(),
                        event -> {
                            if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                                connected.countDown();
                            }
                        });
        if (!connected.await(timeout.toMillis() + 5_000, TimeUnit.MILLISECONDS)) {
            zk.close();
            throw new IOException("Timed out connecting to ZooKeeper at " + connectString);
        }
        return zk;
    }

    // ---- KV workload ops (persistent znodes) ----------------------------------------------------

    @Override
    public CompletableFuture<Void> put(String key, byte[] value) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        String path = KV_ROOT + "/" + key;

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
                                    if (rc2 == KeeperException.Code.OK.intValue()
                                            || rc2 == KeeperException.Code.NODEEXISTS.intValue()) {
                                        future.complete(null);
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
        String path = KV_ROOT + "/" + key;

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

    // ---- session ops ----------------------------------------------------------------------------

    @Override
    public CompletableFuture<SessionHandle> createSession(long logicalId, Duration timeout) {
        // A ZK session is a connection; establishing it is inherently blocking on the SyncConnected
        // event, so do it off the pool thread. Session-establish latency (S2) is measured around this.
        CompletableFuture<SessionHandle> future = new CompletableFuture<>();
        blockingIo.execute(
                () -> {
                    try {
                        future.complete(new ZkSessionHandle(logicalId, connect(connectString, timeout)));
                    } catch (Exception e) {
                        future.completeExceptionally(e);
                    }
                });
        return future;
    }

    @Override
    public CompletableFuture<Void> putEphemeral(SessionHandle session, String key, byte[] value) {
        // Create the ephemeral leaf on the session's own connection so the znode lives and dies with
        // that session. The persistent parent chain is created on demand (see createEphemeral).
        return createEphemeral(((ZkSessionHandle) session).zk(), "/" + key, value, true);
    }

    /** Create the ephemeral leaf; on NONODE (parent missing) create the ancestors and retry once. */
    private CompletableFuture<Void> createEphemeral(
            ZooKeeper zk, String path, byte[] value, boolean retryOnNoNode) {
        CompletableFuture<Void> f = new CompletableFuture<>();
        zk.create(
                path,
                value,
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL,
                (rc, p, ctx, name) -> {
                    if (rc == KeeperException.Code.OK.intValue()
                            || rc == KeeperException.Code.NODEEXISTS.intValue()) {
                        f.complete(null);
                    } else if (rc == KeeperException.Code.NONODE.intValue() && retryOnNoNode) {
                        createAncestors(zk, path)
                                .thenCompose(v -> createEphemeral(zk, path, value, false))
                                .whenComplete(
                                        (v, ex) -> {
                                            if (ex != null) {
                                                f.completeExceptionally(ex);
                                            } else {
                                                f.complete(null);
                                            }
                                        });
                    } else {
                        f.completeExceptionally(KeeperException.create(KeeperException.Code.get(rc), path));
                    }
                },
                null);
        return f;
    }

    /** Blind-create each persistent ancestor of {@code path} in order (NODEEXISTS is fine). */
    private static CompletableFuture<Void> createAncestors(ZooKeeper zk, String path) {
        CompletableFuture<Void> chain = CompletableFuture.completedFuture(null);
        int idx = 0;
        while ((idx = path.indexOf('/', idx + 1)) != -1) {
            String ancestor = path.substring(0, idx);
            chain = chain.thenCompose(v -> createPersistentIfAbsent(zk, ancestor));
        }
        return chain;
    }

    private static CompletableFuture<Void> createPersistentIfAbsent(ZooKeeper zk, String path) {
        CompletableFuture<Void> f = new CompletableFuture<>();
        zk.create(
                path,
                new byte[0],
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT,
                (rc, p, ctx, name) -> {
                    if (rc == KeeperException.Code.OK.intValue()
                            || rc == KeeperException.Code.NODEEXISTS.intValue()) {
                        f.complete(null);
                    } else {
                        f.completeExceptionally(KeeperException.create(KeeperException.Code.get(rc), path));
                    }
                },
                null);
        return f;
    }

    @Override
    public CompletableFuture<Void> closeSession(SessionHandle session) {
        ZooKeeper sessionZk = ((ZkSessionHandle) session).zk();
        return CompletableFuture.runAsync(
                () -> {
                    try {
                        sessionZk.close(); // graceful: sends the close-session request
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
                },
                blockingIo);
    }

    @Override
    public CompletableFuture<Void> killSession(SessionHandle session) {
        ZooKeeper sessionZk = ((ZkSessionHandle) session).zk();
        return CompletableFuture.runAsync(
                () -> {
                    try {
                        // ZooKeeper.cnxn (protected) -> ClientCnxn.disconnect() (public): stop the SendThread
                        // (no more pings, no reconnect) and close the socket WITHOUT sending the close-session
                        // request, so the server reaps the session only via expiry. Never zk.close() here.
                        Field cnxnField = ZooKeeper.class.getDeclaredField("cnxn");
                        cnxnField.setAccessible(true);
                        Object cnxn = cnxnField.get(sessionZk);
                        cnxn.getClass().getMethod("disconnect").invoke(cnxn);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                },
                blockingIo);
    }

    @Override
    public void close() throws IOException {
        try {
            if (zk != null) {
                zk.close();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        } finally {
            if (blockingIo != null) {
                blockingIo.shutdownNow();
            }
        }
    }

    /** ZK session state: its dedicated connection. */
    private record ZkSessionHandle(long logicalId, ZooKeeper zk) implements SessionHandle {}
}
