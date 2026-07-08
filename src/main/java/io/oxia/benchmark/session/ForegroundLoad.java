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

import io.oxia.benchmark.driver.KVStoreDriver;
import io.oxia.benchmark.runner.BenchmarkRunner;
import io.oxia.benchmark.runner.UniformRateLimiter;
import io.oxia.benchmark.runner.sequence.SequenceGenerator;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import org.HdrHistogram.DoubleHistogram;
import org.HdrHistogram.DoubleRecorder;

/**
 * A controllable, rate-paced foreground load — the YCSB-A-style traffic that runs
 * <em>alongside</em> the sessions in S1 (capacity), S3 (load condition), and S4 (storm timeline).
 * It is the session suite's analogue of {@code BenchmarkRunner}'s traffic loop, but
 * startable/stoppable and pollable for interval throughput and p99 so the runner can watch how
 * session activity perturbs it.
 */
public class ForegroundLoad {

    private static final long MAX_LATENCY_MICROS = TimeUnit.SECONDS.toMicros(60);
    private static final int MAX_OUTSTANDING = 10_000; // same in-flight cap as the workload default

    private final KVStoreDriver driver;
    private final double targetRate;
    private final double readRatio;
    private final long keyspaceSize;
    private final String keyDistribution;
    private final int valueSize;
    private final int parallelism;
    private final String[] keys;

    private final DoubleRecorder recorder = new DoubleRecorder(3);
    private final LongAdder failedOps = new LongAdder();
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final List<Thread> workers = new ArrayList<>();

    private DoubleHistogram intervalRecycle;
    private long lastSnapshotNanos = System.nanoTime();
    private long lastFailed;

    public ForegroundLoad(
            KVStoreDriver driver,
            double targetRate,
            double readRatio,
            long keyspaceSize,
            String keyDistribution,
            int valueSize,
            int parallelism) {
        this.driver = driver;
        this.targetRate = targetRate;
        this.readRatio = readRatio;
        this.keyspaceSize = keyspaceSize;
        this.keyDistribution = keyDistribution;
        this.valueSize = valueSize;
        this.parallelism = Math.max(1, parallelism);
        this.keys = BenchmarkRunner.buildKeys(keyspaceSize);
    }

    public boolean active() {
        return targetRate > 0;
    }

    /** Start the load. No-op when the target rate is 0 (session-only experiments). */
    public void start() {
        if (!active() || !running.compareAndSet(false, true)) {
            return;
        }
        for (int i = 0; i < parallelism; i++) {
            Thread t = new Thread(this::generate, "fg-load-" + i);
            t.setDaemon(true);
            t.start();
            workers.add(t);
        }
    }

    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        for (Thread t : workers) {
            try {
                t.join(5_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        workers.clear();
    }

    private void generate() {
        byte[] value = new byte[valueSize];
        Semaphore outstanding = new Semaphore(Math.max(1, MAX_OUTSTANDING / parallelism));
        SequenceGenerator seqGen = SequenceGenerator.create(keyDistribution, keyspaceSize);
        UniformRateLimiter limiter = new UniformRateLimiter(targetRate / parallelism);

        while (running.get()) {
            UniformRateLimiter.sleepUntil(limiter.acquire());
            if (!running.get()) {
                break;
            }
            try {
                outstanding.acquire();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
            if (!running.get()) {
                outstanding.release();
                break;
            }

            long sendTimeNanos = System.nanoTime();
            String key = keys[(int) seqGen.next()];
            boolean isRead = ThreadLocalRandom.current().nextDouble() < readRatio;
            CompletableFuture<Void> future = isRead ? driver.get(key) : driver.put(key, value);
            future.whenComplete(
                    (v, ex) -> {
                        outstanding.release();
                        if (ex != null) {
                            failedOps.increment();
                        } else {
                            long micros =
                                    Math.min(
                                            MAX_LATENCY_MICROS,
                                            TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - sendTimeNanos));
                            recorder.recordValue(micros / 1_000.0);
                        }
                    });
        }
    }

    /**
     * Metrics accumulated since the previous call: interval throughput (ops/s), latency percentiles
     * (ms), and failures. Safe to call while workers record (uses an atomic interval swap).
     */
    public IntervalStats snapshotInterval() {
        long now = System.nanoTime();
        double seconds = (now - lastSnapshotNanos) / 1_000_000_000.0;
        lastSnapshotNanos = now;
        intervalRecycle = recorder.getIntervalHistogram(intervalRecycle);
        long ops = intervalRecycle.getTotalCount();
        long failedNow = failedOps.sum();
        long failedDelta = failedNow - lastFailed;
        lastFailed = failedNow;
        return new IntervalStats(
                seconds > 0 ? ops / seconds : 0,
                ops > 0 ? intervalRecycle.getValueAtPercentile(50) : 0,
                ops > 0 ? intervalRecycle.getValueAtPercentile(99) : 0,
                ops > 0 ? intervalRecycle.getMaxValue() : 0,
                failedDelta);
    }

    /** One interval's foreground metrics (latencies in ms). */
    public record IntervalStats(double throughput, double p50, double p99, double max, long failed) {}
}
