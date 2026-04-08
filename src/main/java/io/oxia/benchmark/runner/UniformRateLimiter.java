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
package io.oxia.benchmark.runner;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * A lock-free rate limiter that distributes permits uniformly over time.
 *
 * <p>Each call to {@link #acquire()} returns the <em>intended</em> send time for the operation,
 * computed as {@code startTime + operationIndex * intervalNs}. The caller is responsible for
 * sleeping until that time. This design (from OpenMessaging Benchmark) enables correct coordinated
 * omission tracking by separating intended time from actual send time.
 */
public class UniformRateLimiter {

    private final long intervalNs;
    private final long startNs;
    private final AtomicLong opIndex = new AtomicLong(0);

    public UniformRateLimiter(double ratePerSecond) {
        this.intervalNs = (long) (1_000_000_000.0 / ratePerSecond);
        this.startNs = System.nanoTime();
    }

    /**
     * Acquires the next permit and returns the intended send time in nanoseconds. The caller should
     * sleep until this time using {@link #sleepUntil(long)}.
     */
    public long acquire() {
        long index = opIndex.getAndIncrement();
        return startNs + index * intervalNs;
    }

    /** Sleeps until the given intended time using {@link LockSupport#parkNanos}. */
    public static void sleepUntil(long intendedTimeNs) {
        long sleepNs;
        while ((sleepNs = intendedTimeNs - System.nanoTime()) > 0) {
            LockSupport.parkNanos(sleepNs);
        }
    }
}
