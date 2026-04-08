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

import com.google.common.util.concurrent.RateLimiter;
import io.oxia.benchmark.driver.KVStoreDriver;
import io.oxia.benchmark.runner.sequence.SequenceGenerator;
import io.prometheus.metrics.core.metrics.Histogram;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import lombok.CustomLog;
import org.HdrHistogram.ConcurrentDoubleHistogram;

@CustomLog
public class BenchmarkRunner {

    private static final Histogram opLatency =
            Histogram.builder()
                    .name("kv_op_latency_seconds")
                    .help("Operation latency")
                    .labelNames("driver", "type", "ok")
                    .nativeOnly()
                    .nativeMaxNumberOfBuckets(10)
                    .register();

    private final Workload workload;
    private final KVStoreDriver driver;

    public BenchmarkRunner(Workload workload, KVStoreDriver driver) {
        this.workload = workload;
        this.driver = driver;
    }

    public void run() throws InterruptedException {
        log.info().attr("workload", workload).log("Running workload");

        SequenceGenerator seqGen =
                SequenceGenerator.create(workload.keyDistribution(), workload.keyspaceSize());

        Duration duration = workload.duration();
        int parallelism = workload.parallelism();

        ConcurrentDoubleHistogram periodWriteHist = new ConcurrentDoubleHistogram(3);
        ConcurrentDoubleHistogram periodReadHist = new ConcurrentDoubleHistogram(3);
        ConcurrentDoubleHistogram totalWriteHist = new ConcurrentDoubleHistogram(3);
        ConcurrentDoubleHistogram totalReadHist = new ConcurrentDoubleHistogram(3);

        LongAdder periodFailedOps = new LongAdder();
        LongAdder totalFailedOps = new LongAdder();

        AtomicBoolean running = new AtomicBoolean(true);

        List<Thread> workers = new ArrayList<>();
        for (int i = 0; i < parallelism; i++) {
            Thread worker =
                    new Thread(
                            () ->
                                    generateTraffic(
                                            seqGen,
                                            running,
                                            periodWriteHist,
                                            periodReadHist,
                                            totalWriteHist,
                                            totalReadHist,
                                            periodFailedOps,
                                            totalFailedOps),
                            "worker-" + i);
            worker.setDaemon(true);
            worker.start();
            workers.add(worker);
        }

        long startNanos = System.nanoTime();
        long endNanos = startNanos + duration.toNanos();
        long statsIntervalNanos = Duration.ofSeconds(10).toNanos();
        long nextStatsNanos = startNanos + statsIntervalNanos;

        while (System.nanoTime() < endNanos) {
            long sleepMs =
                    Math.min(nextStatsNanos - System.nanoTime(), endNanos - System.nanoTime()) / 1_000_000;
            if (sleepMs > 0) {
                Thread.sleep(sleepMs);
            }

            if (System.nanoTime() >= nextStatsNanos) {
                printStats(periodWriteHist, periodReadHist, periodFailedOps, Duration.ofSeconds(10));
                periodWriteHist.reset();
                periodReadHist.reset();
                periodFailedOps.reset();
                nextStatsNanos += statsIntervalNanos;
            }
        }

        running.set(false);
        for (Thread worker : workers) {
            worker.join(5000);
        }

        log.info("Cumulative write/read latencies");
        printStats(
                totalWriteHist,
                totalReadHist,
                totalFailedOps,
                Duration.ofNanos(System.nanoTime() - startNanos));
    }

    @SuppressWarnings("UnstableApiUsage")
    private void generateTraffic(
            SequenceGenerator seqGen,
            AtomicBoolean running,
            ConcurrentDoubleHistogram periodWriteHist,
            ConcurrentDoubleHistogram periodReadHist,
            ConcurrentDoubleHistogram totalWriteHist,
            ConcurrentDoubleHistogram totalReadHist,
            LongAdder periodFailedOps,
            LongAdder totalFailedOps) {

        byte[] value = new byte[workload.valueSize()];
        double perWorkerRate = workload.targetRate() / workload.parallelism();
        long intervalNanos = (long) (1_000_000_000.0 / perWorkerRate);
        RateLimiter limiter = RateLimiter.create(perWorkerRate);

        // Track the intended send time to account for coordinated omission.
        long intendedStartNanos = System.nanoTime();

        while (running.get()) {
            limiter.acquire();
            if (!running.get()) break;

            String key = String.format("k-%016d", seqGen.next());
            long capturedIntended = intendedStartNanos;

            boolean isRead = ThreadLocalRandom.current().nextDouble() < workload.readRatio();
            CompletableFuture<Void> future = isRead ? driver.get(key) : driver.put(key, value);

            future.whenComplete(
                    (v, ex) -> {
                        double latencyMs = (System.nanoTime() - capturedIntended) / 1_000_000.0;
                        if (ex != null) {
                            log.error().exception(ex).log("Operation error");
                            periodFailedOps.increment();
                            totalFailedOps.increment();
                            opLatency
                                    .labelValues(driver.name(), isRead ? "read" : "write", "false")
                                    .observe(latencyMs / 1000.0);
                        } else {
                            if (isRead) {
                                periodReadHist.recordValue(latencyMs);
                                totalReadHist.recordValue(latencyMs);
                            } else {
                                periodWriteHist.recordValue(latencyMs);
                                totalWriteHist.recordValue(latencyMs);
                            }
                            opLatency
                                    .labelValues(driver.name(), isRead ? "read" : "write", "true")
                                    .observe(latencyMs / 1000.0);
                        }
                    });

            intendedStartNanos += intervalNanos;
            // If we're ahead of schedule (system is fast), snap to now
            // to avoid accumulating negative offsets
            long now = System.nanoTime();
            if (intendedStartNanos > now) {
                intendedStartNanos = now;
            }
        }
    }

    private static double safeMax(ConcurrentDoubleHistogram hist) {
        return hist.getTotalCount() == 0 ? 0.0 : hist.getMaxValue();
    }

    private static double safePercentile(ConcurrentDoubleHistogram hist, double percentile) {
        return hist.getTotalCount() == 0 ? 0.0 : hist.getValueAtPercentile(percentile);
    }

    private static void printStats(
            ConcurrentDoubleHistogram writeHist,
            ConcurrentDoubleHistogram readHist,
            LongAdder failedOps,
            Duration period) {
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
