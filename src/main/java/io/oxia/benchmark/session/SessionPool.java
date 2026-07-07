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
package io.oxia.benchmark.session;

import io.oxia.benchmark.driver.session.SessionDriver;
import io.oxia.benchmark.driver.session.SessionHandle;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import lombok.CustomLog;

/**
 * A pool of live sessions run as async state machines — never a thread per session. Each session's
 * lifecycle (establish → live+heartbeating → close/kill) is driven by the backend's own async ops
 * and the {@link AsyncBatch} fan-out; the pool just tracks live handles and rounds work up to a
 * target count with bounded concurrency. "Establish" is create-session + attach {@code k} ephemeral
 * keys — the unit whose latency S2 measures. Keys are derived from the session id via {@link
 * SessionKeys}, so a killed session's keys are reconstructable without its handle (needed by
 * S3/S4).
 */
@CustomLog
public class SessionPool {

    private static final int MAX_RAMP_ROUNDS = 20;

    private final SessionDriver driver;
    private final SessionKeys keys;
    private final int ephemeralsPerSession;
    private final Duration sessionTimeout;
    private final byte[] ephemeralValue;

    private final Map<Long, SessionHandle> live = new ConcurrentHashMap<>();
    private final AtomicLong idAllocator = new AtomicLong();
    private final LongAdder establishFailures = new LongAdder();

    public SessionPool(
            SessionDriver driver,
            SessionKeys keys,
            int ephemeralsPerSession,
            Duration sessionTimeout,
            int ephemeralValueSize) {
        this.driver = driver;
        this.keys = keys;
        this.ephemeralsPerSession = ephemeralsPerSession;
        this.sessionTimeout = sessionTimeout;
        this.ephemeralValue = new byte[ephemeralValueSize];
    }

    public long nextId() {
        return idAllocator.getAndIncrement();
    }

    public int size() {
        return live.size();
    }

    public List<Long> liveIds() {
        return new ArrayList<>(live.keySet());
    }

    public long establishFailures() {
        return establishFailures.sum();
    }

    /**
     * Establish one session: create it and attach its {@code k} ephemeral keys. Adds it to the pool.
     */
    public CompletableFuture<SessionHandle> establish(long id) {
        return driver
                .createSession(id, sessionTimeout)
                .thenCompose(handle -> attachEphemerals(handle).thenApply(v -> handle))
                .thenApply(
                        handle -> {
                            live.put(id, handle);
                            return handle;
                        });
    }

    private CompletableFuture<Void> attachEphemerals(SessionHandle handle) {
        long id = handle.logicalId();
        @SuppressWarnings("unchecked")
        CompletableFuture<Void>[] puts = new CompletableFuture[ephemeralsPerSession];
        for (int j = 0; j < ephemeralsPerSession; j++) {
            puts[j] = driver.putEphemeral(handle, keys.ephemeral(id, j), ephemeralValue);
        }
        return CompletableFuture.allOf(puts);
    }

    /** Gracefully close one live session (protocol goodbye); removes it from the pool. */
    public CompletableFuture<Void> close(long id) {
        SessionHandle h = live.remove(id);
        return h == null ? CompletableFuture.completedFuture(null) : driver.closeSession(h);
    }

    /** Abruptly kill one live session (no goodbye, server-side expiry); removes it from the pool. */
    public CompletableFuture<Void> kill(long id) {
        SessionHandle h = live.remove(id);
        return h == null ? CompletableFuture.completedFuture(null) : driver.killSession(h);
    }

    /**
     * Grow the pool to {@code target} live sessions with at most {@code concurrency} establishes in
     * flight.
     */
    public CompletableFuture<Void> rampTo(long target, int concurrency) {
        return rampRound(target, concurrency, 0);
    }

    private CompletableFuture<Void> rampRound(long target, int concurrency, int round) {
        long need = target - size();
        if (need <= 0) {
            return CompletableFuture.completedFuture(null);
        }
        if (round >= MAX_RAMP_ROUNDS) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException(
                            "Could not reach "
                                    + target
                                    + " sessions after "
                                    + round
                                    + " rounds (have "
                                    + size()
                                    + "); establish failures="
                                    + establishFailures.sum()));
        }
        List<Long> ids = new ArrayList<>((int) Math.min(need, Integer.MAX_VALUE));
        for (long i = 0; i < need; i++) {
            ids.add(nextId());
        }
        return AsyncBatch.run(
                        ids,
                        concurrency,
                        this::establish,
                        (id, ex) -> {
                            establishFailures.increment();
                            log.debug().attr("id", id).exception(ex).log("Session establish failed");
                        })
                .thenCompose(v -> rampRound(target, concurrency, round + 1));
    }

    /** Kill a specific set of live sessions with bounded concurrency (the S4 storm). */
    public CompletableFuture<Void> killIds(List<Long> ids, int concurrency) {
        return AsyncBatch.run(
                ids,
                concurrency,
                this::kill,
                (id, ex) -> log.debug().attr("id", id).exception(ex).log("Session kill failed"));
    }

    /** Gracefully close a specific set of live sessions with bounded concurrency (teardown). */
    public CompletableFuture<Void> closeIds(List<Long> ids, int concurrency) {
        return AsyncBatch.run(
                ids,
                concurrency,
                this::close,
                (id, ex) -> log.debug().attr("id", id).exception(ex).log("Session close failed"));
    }

    /** Gracefully close everything still live (best-effort teardown between sweep points). */
    public CompletableFuture<Void> closeAll(int concurrency) {
        return closeIds(liveIds(), concurrency);
    }
}
