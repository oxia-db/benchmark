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

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * Runs an async operation over a collection with a bounded number of operations in flight — the
 * pool's fan-out primitive. It keeps at most {@code concurrency} operations outstanding and starts
 * the next as each completes, without dedicating a thread per item: the driver ops are async and
 * the only work here is launching the next one from the completion callback. Individual failures
 * are counted (via {@code onError}) rather than aborting the batch, so one bad session never sinks
 * a ramp of thousands.
 */
final class AsyncBatch {

    private AsyncBatch() {}

    static <T> CompletableFuture<Void> run(
            List<T> items,
            int concurrency,
            Function<T, CompletableFuture<?>> op,
            java.util.function.BiConsumer<T, Throwable> onError) {
        CompletableFuture<Void> done = new CompletableFuture<>();
        if (items.isEmpty()) {
            done.complete(null);
            return done;
        }
        new Pump<>(items.iterator(), Math.max(1, concurrency), op, onError, done).pump();
        return done;
    }

    private static final class Pump<T> {
        private final Iterator<T> it;
        private final int concurrency;
        private final Function<T, CompletableFuture<?>> op;
        private final java.util.function.BiConsumer<T, Throwable> onError;
        private final CompletableFuture<Void> done;
        private final AtomicInteger inFlight = new AtomicInteger();
        private boolean exhausted;

        Pump(
                Iterator<T> it,
                int concurrency,
                Function<T, CompletableFuture<?>> op,
                java.util.function.BiConsumer<T, Throwable> onError,
                CompletableFuture<Void> done) {
            this.it = it;
            this.concurrency = concurrency;
            this.op = op;
            this.onError = onError;
            this.done = done;
        }

        synchronized void pump() {
            while (inFlight.get() < concurrency) {
                if (!it.hasNext()) {
                    exhausted = true;
                    break;
                }
                T item = it.next();
                inFlight.incrementAndGet();
                CompletableFuture<?> f;
                try {
                    f = op.apply(item);
                } catch (RuntimeException e) {
                    inFlight.decrementAndGet();
                    onError.accept(item, e);
                    continue;
                }
                f.whenComplete(
                        (v, ex) -> {
                            if (ex != null) {
                                onError.accept(item, ex);
                            }
                            inFlight.decrementAndGet();
                            pump();
                        });
            }
            if (exhausted && inFlight.get() == 0 && !done.isDone()) {
                done.complete(null);
            }
        }
    }
}
