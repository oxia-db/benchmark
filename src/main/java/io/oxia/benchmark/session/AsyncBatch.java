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

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * Runs an async operation over items with at most {@code concurrency} in flight, blocking the
 * calling thread until every operation completes. The driver ops are async (no thread per session);
 * the one orchestrator thread just meters submissions with a semaphore. Individual failures go to
 * {@code onError} rather than aborting the batch, so one bad session never sinks a ramp of
 * thousands.
 */
final class AsyncBatch {

    private AsyncBatch() {}

    static <T> void run(
            List<T> items,
            int concurrency,
            Function<T, CompletableFuture<?>> op,
            BiConsumer<T, Throwable> onError) {
        int permits = Math.max(1, concurrency);
        Semaphore inFlight = new Semaphore(permits);
        for (T item : items) {
            inFlight.acquireUninterruptibly();
            CompletableFuture<?> f;
            try {
                f = op.apply(item);
            } catch (RuntimeException e) {
                inFlight.release();
                onError.accept(item, e);
                continue;
            }
            f.whenComplete(
                    (v, ex) -> {
                        if (ex != null) {
                            onError.accept(item, ex);
                        }
                        inFlight.release();
                    });
        }
        // Reclaiming every permit means the in-flight tail has completed.
        inFlight.acquireUninterruptibly(permits);
    }
}
