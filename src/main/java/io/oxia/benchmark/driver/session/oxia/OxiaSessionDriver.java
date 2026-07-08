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
package io.oxia.benchmark.driver.session.oxia;

import io.oxia.benchmark.driver.session.PrefixListener;
import io.oxia.benchmark.driver.session.SessionDriver;
import io.oxia.benchmark.driver.session.SessionHandle;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.Notification;
import io.oxia.client.api.OxiaClientBuilder;
import io.oxia.client.api.options.PutOption;
import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.CustomLog;

/**
 * Oxia session driver. Oxia binds a session to a client, so each session is its own {@code
 * AsyncOxiaClient} (with a distinct client identifier and the shared session timeout); ephemeral
 * keys are puts tagged {@code AsEphemeralRecord}, and the SDK's KeepAlive heartbeats keep the
 * session live. This mirrors ZooKeeper's one-session-per-connection model, so both pay a
 * per-session client cost the footprint metric captures — versus etcd, which multiplexes leases
 * over one client.
 *
 * <ul>
 *   <li><b>Graceful close</b> — {@code client.close()} sends CloseSession; the server drops the
 *       session and its ephemerals immediately.
 *   <li><b>Abrupt kill</b> — {@link OxiaSessionInternals#kill} cancels the KeepAlive task and shuts
 *       down the gRPC channel <em>without</em> {@code close()}, so no CloseSession is sent and the
 *       server reaps the session only via heartbeat-timeout expiry.
 *   <li><b>Watch</b> — Oxia notifications are a per-client global feed, so the shared client
 *       streams them and we filter by prefix client-side (Oxia has no server-side prefix scoping
 *       for notifications — a disclosed difference from etcd's native prefix watch).
 * </ul>
 */
@CustomLog
public class OxiaSessionDriver implements SessionDriver {

    private String serviceAddress;
    private String namespace;
    private int batchMaxCount;

    private AsyncOxiaClient client; // foreground load + exists() probes + notification feed
    private final AtomicBoolean notificationsStarted = new AtomicBoolean();
    private final CopyOnWriteArrayList<PrefixWatch> watches = new CopyOnWriteArrayList<>();

    @Override
    public String name() {
        return "oxia";
    }

    @Override
    public void init(Map<String, Object> config) throws Exception {
        log.info().attr("config", config).log("Initializing Oxia session driver");
        serviceAddress = (String) config.getOrDefault("serviceAddress", "localhost:6648");
        namespace = (String) config.get("namespace");
        batchMaxCount =
                config.containsKey("batchMaxCount") ? ((Number) config.get("batchMaxCount")).intValue() : 0;
        client = builder("bench-fg").asyncClient().get();
    }

    private OxiaClientBuilder builder(String clientIdentifier) {
        OxiaClientBuilder b =
                OxiaClientBuilder.create(serviceAddress).clientIdentifier(clientIdentifier);
        if (namespace != null) {
            b.namespace(namespace);
        }
        if (batchMaxCount > 0) {
            b.maxRequestsPerBatch(batchMaxCount);
        }
        return b;
    }

    @Override
    public CompletableFuture<Void> put(String key, byte[] value) {
        return client.put(key, value).thenApply(r -> null);
    }

    @Override
    public CompletableFuture<Void> get(String key) {
        return client.get(key).thenApply(r -> null);
    }

    @Override
    public CompletableFuture<SessionHandle> createSession(long logicalId, Duration timeout) {
        // One client == one session. The server-side session is created lazily on the first ephemeral
        // put; establish latency (S2) is measured over createSession + the first putEphemeral together.
        return builder("bench-sess-" + logicalId)
                .sessionTimeout(timeout)
                .asyncClient()
                .thenApply(c -> (SessionHandle) new OxiaSessionHandle(logicalId, c));
    }

    @Override
    public CompletableFuture<Void> putEphemeral(SessionHandle session, String key, byte[] value) {
        AsyncOxiaClient c = ((OxiaSessionHandle) session).client;
        return c.put(key, value, Set.of(PutOption.AsEphemeralRecord)).thenApply(r -> null);
    }

    @Override
    public CompletableFuture<Void> closeSession(SessionHandle session) {
        OxiaSessionHandle h = (OxiaSessionHandle) session;
        return CompletableFuture.runAsync(
                () -> {
                    try {
                        h.client.close(); // graceful: sends CloseSession
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    @Override
    public CompletableFuture<Void> killSession(SessionHandle session) {
        OxiaSessionHandle h = (OxiaSessionHandle) session;
        // Abrupt: stop heartbeats and drop the transport, no CloseSession. Never call client.close().
        return CompletableFuture.runAsync(() -> OxiaSessionInternals.kill(h.client));
    }

    @Override
    public CompletableFuture<Boolean> exists(String key) {
        // Oxia's get() resolves to null when the key is absent (no exception).
        return client.get(key).thenApply(r -> r != null);
    }

    @Override
    public Closeable watchPrefix(String prefix, PrefixListener listener) {
        // Start the global notification stream once, on first use.
        if (notificationsStarted.compareAndSet(false, true)) {
            client.notifications(this::dispatch);
        }
        PrefixWatch w = new PrefixWatch(prefix, listener);
        watches.add(w);
        return () -> watches.remove(w);
    }

    private void dispatch(Notification n) {
        if (!(n instanceof Notification.KeyDeleted) || watches.isEmpty()) {
            return;
        }
        long now = System.nanoTime();
        for (PrefixWatch w : watches) {
            if (n.key().startsWith(w.prefix)) {
                w.listener.onKeyDeleted(n.key(), now);
            }
        }
    }

    @Override
    public void close() throws IOException {
        try {
            if (client != null) {
                client.close();
            }
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    private record PrefixWatch(String prefix, PrefixListener listener) {}

    /** Oxia session state: its dedicated client. */
    private record OxiaSessionHandle(long logicalId, AsyncOxiaClient client)
            implements SessionHandle {}
}
