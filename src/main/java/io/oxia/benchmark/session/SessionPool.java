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
 * lifecycle (establish → live+heartbeating → close/kill) is driven by the backend's own async ops;
 * the pool tracks live handles and fans work out with bounded concurrency via {@link AsyncBatch}.
 * "Establish" is create-session + attach {@code k} ephemeral keys — the unit whose latency the
 * churn experiment measures. Keys derive deterministically from the session id via {@link
 * SessionKeys}.
 */
@CustomLog
public class SessionPool {

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
     * Grow to {@code target} live sessions with at most {@code concurrency} establishes in flight.
     */
    public void rampTo(long target, int concurrency) {
        // A few passes so transient establish failures at scale get retried rather than aborting.
        for (int round = 0; size() < target && round < 3; round++) {
            List<Long> ids = new ArrayList<>();
            for (long i = size(); i < target; i++) {
                ids.add(nextId());
            }
            AsyncBatch.run(
                    ids,
                    concurrency,
                    this::establish,
                    (id, ex) -> {
                        establishFailures.increment();
                        log.debug().attr("id", id).exception(ex).log("Session establish failed");
                    });
        }
        if (size() < target) {
            throw new IllegalStateException(
                    "Could only establish "
                            + size()
                            + " of "
                            + target
                            + " sessions (failures="
                            + establishFailures.sum()
                            + ")");
        }
    }

    /** Gracefully close everything still live (teardown between sweep points). */
    public void closeAll(int concurrency) {
        AsyncBatch.run(
                liveIds(),
                concurrency,
                this::close,
                (id, ex) -> log.debug().attr("id", id).exception(ex).log("Session close failed"));
    }
}
