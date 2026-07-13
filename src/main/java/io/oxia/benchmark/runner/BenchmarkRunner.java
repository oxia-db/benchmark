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

import io.oxia.benchmark.driver.KVStoreDriver;
import io.oxia.benchmark.report.HistogramCodec;
import io.oxia.benchmark.report.WorkloadResult;
import io.oxia.benchmark.runner.sequence.SequenceGenerator;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import lombok.CustomLog;
import org.HdrHistogram.DoubleHistogram;
import org.HdrHistogram.DoubleRecorder;

@CustomLog
public class BenchmarkRunner {

    private static final long MAX_LATENCY_MICROS = TimeUnit.SECONDS.toMicros(60);

    private final Workload workload;
    private final KVStoreDriver driver;

    public BenchmarkRunner(Workload workload, KVStoreDriver driver) {
        this.workload = workload;
        this.driver = driver;
    }

    public WorkloadResult run() throws InterruptedException {
        log.info().attr("workload", workload).log("Running workload");

        SequenceGenerator seqGen =
                SequenceGenerator.create(workload.keyDistribution(), workload.keyspaceSize());

        // Pre-build every key once, so the hot submission loop is a plain array read with no
        // per-op allocation or formatting. The sequence generator returns indices into this array
        // per the configured distribution.
        // Pre-materialize the keyspace only when small enough to fit in heap; otherwise generate
        // each key on the fly in the hot loop (see keyAt).
        String[] keys =
                workload.keyspaceSize() <= PREALLOCATE_KEYS_MAX ? buildKeys(workload.keyspaceSize()) : null;

        Duration warmupDuration = workload.warmup();
        Duration duration = workload.duration();
        int parallelism = workload.parallelism();
        int maxOutstanding = workload.maxOutstandingRequests();
        if (maxOutstanding <= 0) {
            maxOutstanding = 10_000;
        }
        // Give each worker its own semaphore rather than sharing one: a shared semaphore's atomics
        // become a contention point under high throughput. The total in-flight cap is split evenly
        // across the workers.
        int perThreadOutstanding = Math.max(1, maxOutstanding / parallelism);

        // Workers record into DoubleRecorders; the stats loop swaps out interval snapshots
        // atomically (getIntervalHistogram) and accumulates them into the totals, which only
        // this thread touches. Never reset() a histogram that concurrent threads record into:
        // a record racing a reset corrupts the internal value-scaling ratio, yielding absurd
        // latencies (~1e242 ms) and silently dropping subsequent samples.
        DoubleRecorder writeRecorder = new DoubleRecorder(3);
        DoubleRecorder readRecorder = new DoubleRecorder(3);
        DoubleHistogram totalWriteHist = new DoubleHistogram(3);
        DoubleHistogram totalReadHist = new DoubleHistogram(3);
        DoubleHistogram writeInterval = null;
        DoubleHistogram readInterval = null;

        LongAdder periodFailedOps = new LongAdder();
        LongAdder totalFailedOps = new LongAdder();

        AtomicBoolean running = new AtomicBoolean(true);
        AtomicBoolean warmingUp = new AtomicBoolean(!warmupDuration.isZero());

        List<Thread> workers = new ArrayList<>();
        for (int i = 0; i < parallelism; i++) {
            Thread worker =
                    new Thread(
                            () ->
                                    generateTraffic(
                                            seqGen,
                                            keys,
                                            running,
                                            warmingUp,
                                            perThreadOutstanding,
                                            writeRecorder,
                                            readRecorder,
                                            periodFailedOps,
                                            totalFailedOps),
                            "worker-" + i);
            worker.setDaemon(true);
            worker.start();
            workers.add(worker);
        }

        // Warmup phase
        if (!warmupDuration.isZero()) {
            log.info().attr("warmup", warmupDuration).log("Warmup started");
            Thread.sleep(warmupDuration.toMillis());

            // End warmup: drain any straggler samples via an atomic interval swap (safe
            // against concurrent recording) so cumulative stats start clean.
            warmingUp.set(false);
            writeRecorder.getIntervalHistogram();
            readRecorder.getIntervalHistogram();
            periodFailedOps.reset();
            totalFailedOps.reset();
            log.info("Warmup complete, starting measured run");
        }

        long startNanos = System.nanoTime();
        long endNanos = startNanos + duration.toNanos();
        long statsIntervalNanos =
                Duration.ofMillis(Long.getLong("benchmark.statsIntervalMs", 10_000)).toNanos();
        long nextStatsNanos = startNanos + statsIntervalNanos;

        while (System.nanoTime() < endNanos) {
            long sleepMs =
                    Math.min(nextStatsNanos - System.nanoTime(), endNanos - System.nanoTime()) / 1_000_000;
            if (sleepMs > 0) {
                Thread.sleep(sleepMs);
            }

            if (System.nanoTime() >= nextStatsNanos) {
                writeInterval = writeRecorder.getIntervalHistogram(writeInterval);
                readInterval = readRecorder.getIntervalHistogram(readInterval);
                totalWriteHist.add(writeInterval);
                totalReadHist.add(readInterval);
                printStats(
                        writeInterval, readInterval, periodFailedOps, Duration.ofNanos(statsIntervalNanos));
                periodFailedOps.reset();
                nextStatsNanos += statsIntervalNanos;
            }
        }

        running.set(false);
        for (Thread worker : workers) {
            worker.join(5000);
        }

        // Fold the final partial interval into the totals.
        writeInterval = writeRecorder.getIntervalHistogram(writeInterval);
        readInterval = readRecorder.getIntervalHistogram(readInterval);
        totalWriteHist.add(writeInterval);
        totalReadHist.add(readInterval);

        long elapsedNanos = System.nanoTime() - startNanos;
        log.info("Cumulative write/read latencies");
        printStats(totalWriteHist, totalReadHist, totalFailedOps, Duration.ofNanos(elapsedNanos));

        return buildResult(
                elapsedNanos / 1_000_000_000.0, totalWriteHist, totalReadHist, totalFailedOps);
    }

    private WorkloadResult buildResult(
            double measuredSeconds,
            DoubleHistogram writeHist,
            DoubleHistogram readHist,
            LongAdder failedOps) {
        WorkloadResult r = new WorkloadResult();
        r.name = workload.name();
        r.description = workload.description();
        r.hidden = workload.hidden();
        r.driver = driver.name();
        r.readRatio = workload.readRatio();
        r.keyspaceSize = workload.keyspaceSize();
        r.keyDistribution = workload.keyDistribution();
        r.valueSize = workload.valueSize();
        r.targetRate = workload.targetRate();
        r.parallelism = workload.parallelism();
        r.duration = workload.duration().toString();
        r.measuredSeconds = measuredSeconds;
        r.writeCount = writeHist.getTotalCount();
        r.readCount = readHist.getTotalCount();
        r.failedCount = failedOps.sum();
        r.writeHistB64 = HistogramCodec.encode(writeHist);
        r.readHistB64 = HistogramCodec.encode(readHist);
        return r;
    }

    private void generateTraffic(
            SequenceGenerator seqGen,
            String[] keys,
            AtomicBoolean running,
            AtomicBoolean warmingUp,
            int perThreadOutstanding,
            DoubleRecorder writeRecorder,
            DoubleRecorder readRecorder,
            LongAdder periodFailedOps,
            LongAdder totalFailedOps) {

        byte[] zeroValue = new byte[workload.valueSize()];
        // For capacity tests, use incompressible, per-key-distinct payloads so the on-disk dataset
        // reflects the real state size: an all-zero buffer compresses to almost nothing. A pool of
        // immutable random buffers avoids a per-op allocation while keeping each block's values
        // distinct, so block-level compression cannot collapse them. Pool size is a power of two
        // for a cheap masked index.
        byte[][] valuePool = null;
        if (workload.randomValues()) {
            valuePool = new byte[8192][];
            for (int p = 0; p < valuePool.length; p++) {
                byte[] b = new byte[workload.valueSize()];
                ThreadLocalRandom.current().nextBytes(b);
                valuePool[p] = b;
            }
        }
        // This worker's own in-flight cap — private to the thread, so no cross-worker contention.
        Semaphore outstanding = new Semaphore(perThreadOutstanding);
        // targetRate 0 = throughput mode: no pacing, send as fast as the in-flight cap allows.
        boolean rateMode = workload.targetRate() > 0;
        UniformRateLimiter limiter =
                rateMode ? new UniformRateLimiter(workload.targetRate() / workload.parallelism()) : null;

        while (running.get()) {
            if (rateMode) {
                long intendedSendTime = limiter.acquire();
                UniformRateLimiter.sleepUntil(intendedSendTime);
                if (!running.get()) break;
            }

            // Bound concurrent in-flight requests (memory cap; the sole throttle in throughput mode).
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

            boolean isWarmup = warmingUp.get();
            long sendTimeNanos = System.nanoTime();

            long seq = seqGen.next();
            String key = keys != null ? keys[(int) seq] : keyAt(seq);
            byte[] value = valuePool != null ? valuePool[(int) (seq & (valuePool.length - 1))] : zeroValue;
            boolean isRead = ThreadLocalRandom.current().nextDouble() < workload.readRatio();
            CompletableFuture<Void> future = isRead ? driver.get(key) : driver.put(key, value);

            future.whenComplete(
                    (v, ex) -> {
                        outstanding.release();
                        if (isWarmup) return;

                        if (ex != null) {
                            // Count failures (surfaced in Stats + failedCount); don't log a trace
                            // per op — it floods when a throughput run saturates.
                            periodFailedOps.increment();
                            totalFailedOps.increment();
                        } else {
                            // End-to-end latency: actual send time to completion.
                            long latencyMicros =
                                    Math.min(
                                            MAX_LATENCY_MICROS,
                                            TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - sendTimeNanos));
                            double latencyMs = latencyMicros / 1_000.0;
                            if (isRead) {
                                readRecorder.recordValue(latencyMs);
                            } else {
                                writeRecorder.recordValue(latencyMs);
                            }
                        }
                    });
        }
    }

    /**
     * Pre-build all keys up front ("k-" + a 16-digit zero-padded index) so the hot loop only does an
     * array read. Allocates one String per key in the keyspace. Also used by the session suite's
     * foreground load so both traffic loops share one key scheme.
     */
    // Threshold above which the keyspace is not pre-materialized: an array of N key strings costs
    // ~56 bytes each, so 100M keys would need ~5.6 GB per worker and OOM the heap. Below this we
    // pre-build (a cheap array read in the hot loop); above it we format each key on the fly.
    static final long PREALLOCATE_KEYS_MAX = 1_000_000L;

    /** The key for a keyspace index: "k-" followed by the zero-padded 16-digit index. */
    public static String keyAt(long index) {
        char[] c = new char[18];
        c[0] = 'k';
        c[1] = '-';
        long n = index;
        for (int j = 17; j >= 2; j--) {
            c[j] = (char) ('0' + (int) (n % 10));
            n /= 10;
        }
        return new String(c);
    }

    public static String[] buildKeys(long keyspaceSize) {
        String[] keys = new String[(int) keyspaceSize];
        for (int i = 0; i < keys.length; i++) {
            keys[i] = keyAt(i);
        }
        return keys;
    }

    private static double safeMax(DoubleHistogram hist) {
        return hist.getTotalCount() == 0 ? 0.0 : hist.getMaxValue();
    }

    private static double safePercentile(DoubleHistogram hist, double percentile) {
        return hist.getTotalCount() == 0 ? 0.0 : hist.getValueAtPercentile(percentile);
    }

    private static void printStats(
            DoubleHistogram writeHist, DoubleHistogram readHist, LongAdder failedOps, Duration period) {
        double periodSec = period.toNanos() / 1_000_000_000.0;
        double writeRate = writeHist.getTotalCount() / periodSec;
        double readRate = readHist.getTotalCount() / periodSec;
        double failedRate = failedOps.sum() / periodSec;

        log.infof(
                "Stats - Total ops: %6.1f ops/s - Failed ops: %6.1f ops/s%n"
                        + "                Write ops %6.1f w/s  Latency ms: 50%% %5.1f - 95%% %5.1f - 99%%"
                        + " %5.1f - 99.9%% %5.1f - max %6.1f%n"
                        + "                Read  ops %6.1f r/s  Latency ms: 50%% %5.1f - 95%% %5.1f - 99%%"
                        + " %5.1f - 99.9%% %5.1f - max %6.1f",
                writeRate + readRate,
                failedRate,
                writeRate,
                safePercentile(writeHist, 50),
                safePercentile(writeHist, 95),
                safePercentile(writeHist, 99),
                safePercentile(writeHist, 99.9),
                safeMax(writeHist),
                readRate,
                safePercentile(readHist, 50),
                safePercentile(readHist, 95),
                safePercentile(readHist, 99),
                safePercentile(readHist, 99.9),
                safeMax(readHist));
    }
}
