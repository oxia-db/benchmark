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
package io.oxia.benchmark.driver.session;

import io.oxia.benchmark.driver.KVStoreDriver;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * A {@link KVStoreDriver} that additionally exposes the session / ephemeral primitives the session
 * experiments (capacity, churn) drive. The inherited {@link #put}/{@link #get} run the foreground
 * (non-session) load; the methods here manage sessions and their attached ephemeral keys.
 *
 * <p><b>Fairness contract.</b> Every method takes the same session timeout across systems and each
 * backend heartbeats at {@code timeout/3} (Oxia's SDK KeepAlive, ZooKeeper's SendThread ping,
 * etcd's lease keep-alive stream). Where a system cannot express an operation natively, the closest
 * contract-equivalent recipe is used and the difference is disclosed in the README. The mapping is:
 *
 * <ul>
 *   <li><b>Oxia</b> — one {@code AsyncOxiaClient} per session (its own KeepAlive heartbeats);
 *       ephemeral records via {@code PutOption.AsEphemeralRecord}.
 *   <li><b>ZooKeeper</b> — one {@code ZooKeeper} handle per session (ZK's model: one session per
 *       TCP connection); ephemeral znodes.
 *   <li><b>etcd</b> — one lease per session (leases multiplexed over a shared client), puts
 *       attached to the lease id, kept alive by a {@code LeaseKeepAlive} stream.
 * </ul>
 *
 * <p>Sessions are async state machines driven from a shared pool (no thread per session). Handles
 * are cheap tokens; the backend cost (sockets, threads) is measured, not hidden — see {@code
 * Footprint}.
 */
public interface SessionDriver extends KVStoreDriver {

    /**
     * Establish a session with the given timeout and heartbeat cadence of {@code timeout/3}. The
     * returned handle carries no ephemeral keys yet; attach them with {@link #putEphemeral}. Never
     * blocks a thread waiting on the network.
     */
    CompletableFuture<SessionHandle> createSession(long logicalId, Duration timeout);

    /**
     * Attach an ephemeral key to a session: the key lives exactly as long as the session does and is
     * removed by the server when the session closes or expires.
     */
    CompletableFuture<Void> putEphemeral(SessionHandle session, String key, byte[] value);

    /**
     * Graceful departure: send the protocol goodbye (Oxia CloseSession / ZK close / etcd lease
     * revoke) so the server drops the session and its ephemerals immediately.
     */
    CompletableFuture<Void> closeSession(SessionHandle session);

    /**
     * Abrupt departure — simulate a crashed client. Stop heartbeats and tear down the transport
     * <em>without</em> the protocol goodbye, so the server only learns of the departure via the
     * expiry (timeout) path. For ZooKeeper this closes the socket without calling {@code close()};
     * for Oxia it cancels the KeepAlive and drops the channel; for etcd it stops the keep-alive
     * stream without revoking the lease.
     */
    CompletableFuture<Void> killSession(SessionHandle session);
}
