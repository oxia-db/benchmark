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
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * An in-memory {@link SessionDriver} that emulates real session semantics well enough to test the
 * pool and the S1/S2 runner deterministically: ephemeral keys become visible on put, a graceful
 * close removes them at once, and an abrupt kill schedules their removal after the session timeout
 * — the server-side expiry path. No real cluster needed.
 */
class MockSessionDriver implements SessionDriver {

    private final ScheduledExecutorService sched =
            Executors.newScheduledThreadPool(
                    2,
                    r -> {
                        Thread t = new Thread(r, "mock-expiry");
                        t.setDaemon(true);
                        return t;
                    });

    private final java.util.Set<String> present = ConcurrentHashMap.newKeySet();
    private final Map<Long, List<String>> sessionKeys = new ConcurrentHashMap<>();
    private final Map<Long, Duration> timeouts = new ConcurrentHashMap<>();

    @Override
    public String name() {
        return "mock";
    }

    @Override
    public void init(Map<String, Object> config) {}

    @Override
    public CompletableFuture<Void> put(String key, byte[] value) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> get(String key) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<SessionHandle> createSession(long logicalId, Duration timeout) {
        timeouts.put(logicalId, timeout);
        sessionKeys.put(logicalId, new CopyOnWriteArrayList<>());
        return CompletableFuture.completedFuture(new MockHandle(logicalId));
    }

    @Override
    public CompletableFuture<Void> putEphemeral(SessionHandle session, String key, byte[] value) {
        present.add(key);
        sessionKeys.computeIfAbsent(session.logicalId(), id -> new CopyOnWriteArrayList<>()).add(key);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> closeSession(SessionHandle session) {
        removeSessionKeys(session.logicalId()); // graceful: immediate
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> killSession(SessionHandle session) {
        long id = session.logicalId();
        Duration timeout = timeouts.getOrDefault(id, Duration.ofSeconds(1));
        // Server-side expiry: keys survive until the timeout lapses after heartbeats stop.
        sched.schedule(() -> removeSessionKeys(id), timeout.toMillis(), TimeUnit.MILLISECONDS);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void close() {
        sched.shutdownNow();
    }

    private void removeSessionKeys(long id) {
        List<String> keys = sessionKeys.remove(id);
        if (keys == null) {
            return;
        }
        keys.forEach(present::remove);
    }

    int presentCount() {
        return present.size();
    }

    private record MockHandle(long logicalId) implements SessionHandle {}
}
