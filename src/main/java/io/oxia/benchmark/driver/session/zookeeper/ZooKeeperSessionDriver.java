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
package io.oxia.benchmark.driver.session.zookeeper;

import io.oxia.benchmark.driver.session.PrefixListener;
import io.oxia.benchmark.driver.session.SessionDriver;
import io.oxia.benchmark.driver.session.SessionHandle;
import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.CustomLog;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

/**
 * ZooKeeper session driver. ZK's model <em>is</em> one session per client / TCP connection, so we
 * keep it: each session is its own {@link ZooKeeper} handle, and ephemeral keys are ephemeral
 * znodes created on that handle. The SendThread pings the server at ~timeout/3 automatically, so
 * the heartbeat cadence matches the other systems by construction. The negotiated timeout is
 * bounded by the server's {@code tickTime} (min 2·tick, max 20·tick); configure {@code tickTime} so
 * the chosen timeout is admissible (10s is fine with the default 2s tick).
 *
 * <ul>
 *   <li><b>Graceful close</b> — {@code ZooKeeper.close()} sends the close-session request; the
 *       server drops the session and its ephemerals at once.
 *   <li><b>Abrupt kill</b> — {@code ClientCnxn.disconnect()} (reached reflectively) stops the
 *       SendThread and closes the socket <em>without</em> the close-session request, so the server
 *       only reaps the session via the expiry path. Calling {@code close()} would instead end the
 *       session gracefully and skip that path, which is exactly what we must avoid.
 *   <li><b>Watch</b> — classic child watches with re-registration: a one-shot {@code getChildren}
 *       watch on the parent znode, re-armed on every fire, diffing the child set to surface
 *       deletions. ZK's notification says only "children changed", so identifying <em>which</em>
 *       key was removed costs the extra re-read — a disclosed, native property reflected in the
 *       measured latency.
 * </ul>
 */
@CustomLog
public class ZooKeeperSessionDriver implements SessionDriver {

    private static final Duration OBSERVER_TIMEOUT = Duration.ofSeconds(30);
    private static final String FOREGROUND_ROOT = "/bench-fg";

    private String connectString;
    private ZooKeeper observer; // foreground load + exists() probes + prefix watches
    private ExecutorService
            blockingIo; // ZK close()/disconnect() can block; keep them off pool threads
    private final Set<String> ensuredPaths = ConcurrentHashMap.newKeySet();

    @Override
    public String name() {
        return "zookeeper";
    }

    @Override
    public void init(Map<String, Object> config) throws Exception {
        log.info().attr("config", config).log("Initializing ZooKeeper session driver");

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

        observer = connect(connectString, OBSERVER_TIMEOUT);
        ensurePersistentPath(observer, FOREGROUND_ROOT);
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

    // ---- foreground KV (persistent znodes), mirroring the throughput ZooKeeperDriver -------------

    @Override
    public CompletableFuture<Void> put(String key, byte[] value) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        String path = FOREGROUND_ROOT + "/" + key;
        observer.setData(
                path,
                value,
                -1,
                (rc, p, ctx, stat) -> {
                    if (rc == KeeperException.Code.OK.intValue()) {
                        future.complete(null);
                    } else if (rc == KeeperException.Code.NONODE.intValue()) {
                        observer.create(
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
        String path = FOREGROUND_ROOT + "/" + key;
        observer.getData(
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

    // ---- sessions -------------------------------------------------------------------------------

    @Override
    public CompletableFuture<SessionHandle> createSession(long logicalId, Duration timeout) {
        // A ZK session is a connection; establishing it is inherently blocking on the SyncConnected
        // event, so do it off the pool thread. Session-establish latency (S2) is measured around this.
        CompletableFuture<SessionHandle> future = new CompletableFuture<>();
        blockingIo.execute(
                () -> {
                    try {
                        ZooKeeper zk = connect(connectString, timeout);
                        future.complete(new ZkSessionHandle(logicalId, zk));
                    } catch (Exception e) {
                        future.completeExceptionally(e);
                    }
                });
        return future;
    }

    @Override
    public CompletableFuture<Void> putEphemeral(SessionHandle session, String key, byte[] value) {
        ZooKeeper zk = ((ZkSessionHandle) session).zk;
        String path = "/" + key;
        String parent = path.substring(0, path.lastIndexOf('/'));
        // Ensure the persistent ancestor chain, then create the ephemeral leaf on the session's own
        // connection so the znode lives and dies with that session.
        return ensurePath(zk, parent).thenCompose(v -> createEphemeral(zk, path, value));
    }

    private CompletableFuture<Void> createEphemeral(ZooKeeper zk, String path, byte[] value) {
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
                    } else {
                        f.completeExceptionally(KeeperException.create(KeeperException.Code.get(rc), path));
                    }
                },
                null);
        return f;
    }

    @Override
    public CompletableFuture<Void> closeSession(SessionHandle session) {
        ZooKeeper zk = ((ZkSessionHandle) session).zk;
        return CompletableFuture.runAsync(
                () -> {
                    try {
                        zk.close(); // graceful: sends the close-session request
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
                },
                blockingIo);
    }

    @Override
    public CompletableFuture<Void> killSession(SessionHandle session) {
        ZooKeeper zk = ((ZkSessionHandle) session).zk;
        return CompletableFuture.runAsync(
                () -> {
                    try {
                        // Reach the ClientCnxn and disconnect(): stops the SendThread (no more pings, no
                        // reconnect) and closes the socket WITHOUT sending the close-session request, so the
                        // server reaps the session only via expiry. We deliberately never call zk.close() here.
                        disconnectAbruptly(zk);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                },
                blockingIo);
    }

    private void disconnectAbruptly(ZooKeeper zk) throws ReflectiveOperationException {
        // ZooKeeper.cnxn (protected) -> ClientCnxn.disconnect() (public): stop the SendThread and close
        // the socket without enqueuing a close-session packet. Verified against the pinned zookeeper
        // version in build.gradle.kts.
        Field cnxnField = ZooKeeper.class.getDeclaredField("cnxn");
        cnxnField.setAccessible(true);
        Object cnxn = cnxnField.get(zk);
        cnxn.getClass().getMethod("disconnect").invoke(cnxn);
    }

    @Override
    public CompletableFuture<Boolean> exists(String key) {
        CompletableFuture<Boolean> f = new CompletableFuture<>();
        String path = "/" + key;
        observer.exists(
                path,
                false,
                (rc, p, ctx, stat) -> {
                    if (rc == KeeperException.Code.OK.intValue()) {
                        f.complete(true);
                    } else if (rc == KeeperException.Code.NONODE.intValue()) {
                        f.complete(false);
                    } else {
                        f.completeExceptionally(KeeperException.create(KeeperException.Code.get(rc), path));
                    }
                },
                null);
        return f;
    }

    @Override
    public Closeable watchPrefix(String prefix, PrefixListener listener) {
        // Classic child-watch with re-registration. The prefix is interpreted as the parent znode
        // whose ephemeral children we observe; deletions are found by diffing the child set on each
        // re-read (ZK tells us only that children changed, not which one).
        String parent = "/" + trimTrailingSlash(prefix);
        ChildWatch watch = new ChildWatch(parent, listener);
        watch.arm();
        return watch;
    }

    private static String trimTrailingSlash(String s) {
        return s.endsWith("/") ? s.substring(0, s.length() - 1) : s;
    }

    /**
     * Re-registering child watcher: diffs the child set on each fire to emit create/delete events.
     */
    private final class ChildWatch implements Watcher, Closeable {
        private final String parent;
        private final PrefixListener listener;
        private final AtomicBoolean active = new AtomicBoolean(true);
        private volatile Set<String> known = new HashSet<>();

        ChildWatch(String parent, PrefixListener listener) {
            this.parent = parent;
            this.listener = listener;
        }

        void arm() {
            if (!active.get()) {
                return;
            }
            observer.getChildren(
                    parent,
                    this,
                    (rc, p, ctx, children, stat) -> {
                        long now = System.nanoTime();
                        if (rc != KeeperException.Code.OK.intValue()) {
                            return; // parent gone or transient error: stop quietly
                        }
                        Set<String> current = new HashSet<>(children);
                        Set<String> prev = known;
                        for (String c : prev) {
                            if (!current.contains(c)) {
                                listener.onKeyDeleted(childKey(c), now);
                            }
                        }
                        for (String c : current) {
                            if (!prev.contains(c)) {
                                listener.onKeyCreated(childKey(c), now);
                            }
                        }
                        known = current;
                    },
                    null);
        }

        private String childKey(String child) {
            // Strip the leading '/' so keys read back the same across systems.
            return (parent + "/" + child).substring(1);
        }

        @Override
        public void process(WatchedEvent event) {
            if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                arm(); // re-register and re-read to discover what changed
            }
        }

        @Override
        public void close() {
            active.set(false);
        }
    }

    // ---- persistent-path helpers ----------------------------------------------------------------

    /**
     * Async-ensure the full ancestor chain of a path exists as persistent znodes, cached once seen.
     */
    private CompletableFuture<Void> ensurePath(ZooKeeper zk, String path) {
        if (path.isEmpty() || path.equals("/") || ensuredPaths.contains(path)) {
            return CompletableFuture.completedFuture(null);
        }
        String parent = path.substring(0, path.lastIndexOf('/'));
        return ensurePath(zk, parent)
                .thenCompose(v -> createPersistentIfAbsent(zk, path))
                .thenRun(() -> ensuredPaths.add(path));
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

    private void ensurePersistentPath(ZooKeeper zk, String path) throws Exception {
        ensurePath(zk, path).get(OBSERVER_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() throws IOException {
        try {
            if (observer != null) {
                observer.close();
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
    private static final class ZkSessionHandle extends SessionHandle {
        private final ZooKeeper zk;

        ZkSessionHandle(long logicalId, ZooKeeper zk) {
            super(logicalId);
            this.zk = zk;
        }

        @Override
        public long backendId() {
            return zk.getSessionId();
        }
    }
}
