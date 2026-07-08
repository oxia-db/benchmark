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
package io.oxia.benchmark.driver.session.etcd;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.Lease;
import io.etcd.jetcd.lease.LeaseKeepAliveResponse;
import io.etcd.jetcd.options.PutOption;
import io.etcd.jetcd.support.CloseableClient;
import io.grpc.stub.StreamObserver;
import io.oxia.benchmark.driver.session.SessionDriver;
import io.oxia.benchmark.driver.session.SessionHandle;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.CustomLog;

/**
 * etcd session driver. A "session" is a lease with a TTL: ephemeral keys are puts attached to the
 * lease id, and a {@code LeaseKeepAlive} stream renews the lease. Unlike Oxia and ZooKeeper — where
 * a session is bound to a client/connection — etcd multiplexes every lease over one shared client
 * (jetcd shares a single gRPC connection). That is the disclosed model difference: etcd's
 * per-session socket cost is amortized, which the footprint metric will show, so we keep the native
 * model rather than forcing a connection per lease.
 *
 * <p>Graceful close revokes the lease (immediate key removal); abrupt kill only stops the
 * keep-alive stream, leaving the server to expire the lease after its TTL — the closest equivalent
 * to a crashed client, since there is no per-lease socket to drop.
 */
@CustomLog
public class EtcdSessionDriver implements SessionDriver {

    private Client client;
    private KV kvClient;
    private Lease leaseClient;

    @Override
    public String name() {
        return "etcd";
    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(Map<String, Object> config) throws Exception {
        log.info().attr("config", config).log("Initializing etcd session driver");

        Object endpointsObj = config.get("endpoints");
        if (endpointsObj == null) {
            throw new IllegalArgumentException("Missing 'endpoints' in etcd config");
        }
        if (!(endpointsObj instanceof List<?> list)) {
            throw new IllegalArgumentException(
                    "Invalid type for 'endpoints': " + endpointsObj.getClass());
        }
        String[] targets = list.stream().map(e -> "http://" + e).toArray(String[]::new);

        // See EtcdDriver for why only the stable core builder API is used (shaded jetcd copy).
        client = Client.builder().endpoints(targets).build();
        kvClient = client.getKVClient();
        leaseClient = client.getLeaseClient();
    }

    @Override
    public CompletableFuture<Void> put(String key, byte[] value) {
        return kvClient
                .put(ByteSequence.from(key, StandardCharsets.UTF_8), ByteSequence.from(value))
                .thenApply(r -> null);
    }

    @Override
    public CompletableFuture<Void> get(String key) {
        return kvClient.get(ByteSequence.from(key, StandardCharsets.UTF_8)).thenApply(r -> null);
    }

    @Override
    public CompletableFuture<SessionHandle> createSession(long logicalId, Duration timeout) {
        // etcd lease TTL is granted in whole seconds; round up so it is never shorter than the
        // requested timeout. The server keeps the lease alive at ~TTL/3 via the stream below.
        long ttlSeconds = Math.max(1, (long) Math.ceil(timeout.toMillis() / 1000.0));
        return leaseClient
                .grant(ttlSeconds)
                .thenApply(
                        grant -> {
                            long leaseId = grant.getID();
                            // The keep-alive stream renews the lease for as long as it stays open; jetcd drives
                            // the heartbeat cadence internally (~TTL/3). Closing the stream (kill) stops
                            // renewals so the lease expires after the TTL.
                            CloseableClient keepAlive =
                                    leaseClient.keepAlive(leaseId, new NoopKeepAliveObserver());
                            return new EtcdSessionHandle(logicalId, leaseId, keepAlive);
                        });
    }

    @Override
    public CompletableFuture<Void> putEphemeral(SessionHandle session, String key, byte[] value) {
        long leaseId = ((EtcdSessionHandle) session).leaseId;
        PutOption opt = PutOption.builder().withLeaseId(leaseId).build();
        return kvClient
                .put(ByteSequence.from(key, StandardCharsets.UTF_8), ByteSequence.from(value), opt)
                .thenApply(r -> null);
    }

    @Override
    public CompletableFuture<Void> closeSession(SessionHandle session) {
        EtcdSessionHandle h = (EtcdSessionHandle) session;
        h.keepAlive.close();
        // Revoke = graceful goodbye: the server removes the lease and its attached keys at once.
        return leaseClient.revoke(h.leaseId).thenApply(r -> null);
    }

    @Override
    public CompletableFuture<Void> killSession(SessionHandle session) {
        EtcdSessionHandle h = (EtcdSessionHandle) session;
        // Abrupt: stop heartbeats (close the keep-alive stream) but do NOT revoke. The server only
        // learns of the departure when the lease TTL lapses — the expiry path we want to measure.
        h.keepAlive.close();
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void close() throws IOException {
        if (leaseClient != null) {
            leaseClient.close();
        }
        if (kvClient != null) {
            kvClient.close();
        }
        if (client != null) {
            client.close();
        }
    }

    /** etcd session state: the lease id and its keep-alive stream. */
    private record EtcdSessionHandle(long logicalId, long leaseId, CloseableClient keepAlive)
            implements SessionHandle {}

    /**
     * The keep-alive responses carry the server-echoed TTL; the benchmark drives expiry by opening
     * and closing the stream, so the individual renewals need no handling. Errors are expected on
     * kill (the stream is force-closed) and on shutdown, so they are swallowed quietly.
     */
    private static final class NoopKeepAliveObserver
            implements StreamObserver<LeaseKeepAliveResponse> {
        @Override
        public void onNext(LeaseKeepAliveResponse value) {}

        @Override
        public void onError(Throwable t) {}

        @Override
        public void onCompleted() {}
    }
}
