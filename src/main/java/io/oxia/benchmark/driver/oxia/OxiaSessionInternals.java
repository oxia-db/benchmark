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
package io.oxia.benchmark.driver.oxia;

import io.oxia.client.api.AsyncOxiaClient;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Simulates an abrupt client death for an Oxia session by reaching into SDK internals. The public
 * {@code AsyncOxiaClient} exposes only {@code close()}, which sends a graceful CloseSession and
 * skips the server-side expiry path — the exact path the churn "abandon" departure must exercise.
 * So, to model a crashed client, we instead:
 *
 * <ol>
 *   <li>cancel each shard {@code Session}'s KeepAlive task and mark it closed, stopping heartbeats;
 *   <li>close the client's batcher threads — a purely local teardown (pending ops fail without
 *       anything being sent on the wire), so abandoned clients don't leak two threads each.
 * </ol>
 *
 * <p>The transport is left alone: the benchmark's clients share connections via {@code
 * SharedResources} (oxia-client 0.9.1+), so there is no per-session socket to drop — the same
 * disclosed model as etcd's leases. The server then reaps the session only after the heartbeat
 * timeout, as with a real crash. Field names are pinned to the {@code
 * io.github.oxia-db:oxia-client} version in build.gradle.kts; if the internals move, {@link #kill}
 * throws so a run fails loudly rather than silently degrading a "kill" into a no-op (which would
 * corrupt the expiry measurements).
 */
final class OxiaSessionInternals {

    private OxiaSessionInternals() {}

    static void kill(AsyncOxiaClient client) {
        try {
            stopHeartbeats(client);
            closeBatchers(client);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(
                    "Failed to abruptly kill Oxia session via SDK internals; the oxia-client internal"
                            + " layout may have changed and the kill path needs updating",
                    e);
        }
    }

    /**
     * Cancel the per-shard KeepAlive futures and mark sessions closed so no more heartbeats go out.
     */
    @SuppressWarnings("unchecked")
    private static void stopHeartbeats(AsyncOxiaClient client) throws ReflectiveOperationException {
        Object sessionManager = get(client, "sessionManager");
        Object sessionsObj = get(sessionManager, "sessions");
        if (!(sessionsObj instanceof Map<?, ?> sessions)) {
            return;
        }
        for (Object value : ((Map<Object, Object>) sessions).values()) {
            Object session = value;
            if (value instanceof CompletableFuture<?> f) {
                if (!f.isDone() || f.isCompletedExceptionally()) {
                    continue; // session still being created or failed; nothing to stop
                }
                session = f.getNow(null);
            }
            if (session == null) {
                continue;
            }
            Object hb = get(session, "heartbeatFuture");
            if (hb instanceof Future<?> future) {
                future.cancel(true);
            }
            Object closed = get(session, "closed");
            if (closed instanceof AtomicBoolean flag) {
                flag.set(true);
            }
        }
    }

    /** Stop the client's read/write batcher threads (local only; nothing goes on the wire). */
    private static void closeBatchers(AsyncOxiaClient client) throws ReflectiveOperationException {
        for (String field : new String[] {"readBatchManager", "writeBatchManager"}) {
            Object manager = get(client, field);
            if (manager instanceof AutoCloseable closeable) {
                try {
                    closeable.close();
                } catch (Exception ignored) {
                    // best-effort thread reaping; the heartbeat stop above is what matters
                }
            }
        }
    }

    /** Read a (possibly inherited) declared field, making it accessible. */
    private static Object get(Object target, String fieldName) throws ReflectiveOperationException {
        Class<?> c = target.getClass();
        while (c != null) {
            try {
                Field f = c.getDeclaredField(fieldName);
                f.setAccessible(true);
                return f.get(target);
            } catch (NoSuchFieldException e) {
                c = c.getSuperclass();
            }
        }
        throw new NoSuchFieldException(fieldName + " on " + target.getClass());
    }
}
