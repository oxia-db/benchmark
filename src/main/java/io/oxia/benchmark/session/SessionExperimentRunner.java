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
import io.oxia.benchmark.runner.UniformRateLimiter;
import io.oxia.benchmark.session.SessionResult.CapacityPoint;
import io.oxia.benchmark.session.SessionResult.ChurnPoint;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import lombok.CustomLog;
import org.HdrHistogram.DoubleHistogram;
import org.HdrHistogram.DoubleRecorder;

/**
 * Runs one {@link SessionExperiment} (S1 capacity, S2 churn) against a {@link SessionDriver} and
 * returns a {@link SessionResult}. The orchestration runs synchronously on this one thread and
 * drives thousands of sessions through the async {@link SessionPool}; the sessions themselves are
 * never thread-backed. Every experiment uses the identical session timeout and ephemeral count from
 * the config, so results are comparable across backends.
 */
@CustomLog
public class SessionExperimentRunner {

    private final SessionExperiment exp;
    private final SessionDriver driver;
    private final String instanceId;
    private final SessionKeys keys;
    private final Duration timeout;
    private final int concurrency;

    public SessionExperimentRunner(SessionExperiment exp, SessionDriver driver, String instanceId) {
        this.exp = exp;
        this.driver = driver;
        this.instanceId = instanceId == null ? "local" : instanceId;
        // Namespace keys per worker so multiple workers against one cluster never collide.
        this.keys = new SessionKeys(exp.keyPrefix() + "/" + this.instanceId);
        this.timeout = exp.sessionTimeout();
        this.concurrency = exp.createConcurrency();
    }

    public SessionResult run() throws Exception {
        log.info().attr("experiment", exp).log("Running session experiment");
        SessionResult r = new SessionResult();
        r.instanceId = instanceId;
        r.name = exp.name();
        r.description = exp.description();
        r.type = exp.type();
        r.driver = driver.name();
        r.sessionTimeout = exp.sessionTimeout().toString();
        r.sessionTimeoutMs = timeout.toMillis();
        r.ephemeralsPerSession = exp.ephemeralsPerSession();

        switch (exp.type()) {
            case "S1" -> runCapacity(r);
            case "S2" -> runChurn(r);
            default -> throw new IllegalArgumentException("unknown experiment type: " + exp.type());
        }
        return r;
    }

    private SessionPool newPool() {
        return new SessionPool(
                driver, keys, exp.ephemeralsPerSession(), timeout, exp.ephemeralValueSize());
    }

    private ForegroundLoad newForeground() {
        return new ForegroundLoad(
                driver,
                exp.foregroundRate(),
                exp.foregroundReadRatio(),
                exp.foregroundKeyspaceSize(),
                exp.foregroundKeyDistribution(),
                exp.foregroundValueSize(),
                exp.foregroundParallelism());
    }

    // ---- S1 capacity ----------------------------------------------------------------------------

    private void runCapacity(SessionResult r) {
        r.capacity = new ArrayList<>();
        SessionPool pool = newPool();
        ForegroundLoad fg = newForeground();
        fg.start();
        sleep(exp.warmup());

        // Baseline: foreground alone (N=0). This anchors both throughput degradation and footprint.
        Footprint baseFp = Footprint.sample();
        fg.snapshotInterval(); // reset window
        sleep(exp.holdDuration());
        ForegroundLoad.IntervalStats base = fg.snapshotInterval();
        double baseline = base.throughput();
        r.capacity.add(capacityPoint(0, base, 0.0, baseFp.minus(baseFp)));
        log.info()
                .attr("baselineThroughput", String.format("%.0f", baseline))
                .attr("baselineP99", String.format("%.1f", base.p99()))
                .log("S1 baseline (N=0) measured");

        for (long n : exp.sessionsSweep()) {
            pool.rampTo(n, concurrency);
            sleep(exp.warmup()); // let heartbeat/connection churn settle before measuring
            fg.snapshotInterval(); // reset window
            sleep(exp.holdDuration());
            ForegroundLoad.IntervalStats s = fg.snapshotInterval();
            Footprint per10k = Footprint.sample().minus(baseFp).per10k(n);
            double degradation = baseline > 0 ? (baseline - s.throughput()) / baseline * 100.0 : 0.0;
            r.capacity.add(capacityPoint(n, s, degradation, per10k));
            log.info()
                    .attr("N", n)
                    .attr("throughput", String.format("%.0f", s.throughput()))
                    .attr("p99ms", String.format("%.1f", s.p99()))
                    .attr("degradationPct", String.format("%.1f", degradation))
                    .attr("threadsPer10k", per10k.threads())
                    .attr("heapMBPer10k", String.format("%.1f", per10k.heapMB()))
                    .log("S1 sweep point");
        }
        fg.stop();
        pool.closeAll(concurrency);
    }

    private CapacityPoint capacityPoint(
            long n, ForegroundLoad.IntervalStats s, double degradation, Footprint per10k) {
        CapacityPoint p = new CapacityPoint();
        p.sessions = n;
        p.foregroundThroughput = s.throughput();
        p.foregroundP50 = s.p50();
        p.foregroundP99 = s.p99();
        p.foregroundMax = s.max();
        p.foregroundFailed = s.failed();
        p.degradationPct = degradation;
        p.footprintSocketsPer10k = per10k.sockets();
        p.footprintThreadsPer10k = per10k.threads();
        p.footprintHeapMBPer10k = per10k.heapMB();
        return p;
    }

    // ---- S2 churn -------------------------------------------------------------------------------

    private void runChurn(SessionResult r) {
        r.churn = new ArrayList<>();
        r.departure = exp.departure();
        boolean graceful = exp.departure().equals("graceful");
        ForegroundLoad fg = newForeground();
        fg.start();
        sleep(exp.warmup());
        // One pool for the whole sweep so session ids (and thus ephemeral keys) stay globally unique
        // even while keys from a previous rate's abandoned sessions are still expiring server-side.
        SessionPool pool = newPool();
        for (double rate : exp.churnRateSweep()) {
            r.churn.add(runChurnRate(pool, rate, graceful));
            pool.closeAll(concurrency);
        }
        fg.stop();
    }

    private ChurnPoint runChurnRate(SessionPool pool, double rate, boolean graceful) {
        Semaphore inFlight = new Semaphore(concurrency);
        ArrayDeque<Long> order = new ArrayDeque<>();
        DoubleRecorder establish = new DoubleRecorder(3);
        LongAdder created = new LongAdder();
        LongAdder failed = new LongAdder();
        LongAdder departed = new LongAdder();
        long fellBehind = 0;

        UniformRateLimiter limiter = new UniformRateLimiter(rate);
        long startNanos = System.nanoTime();
        long endNanos = startNanos + exp.holdDuration().toNanos();

        while (System.nanoTime() < endNanos) {
            UniformRateLimiter.sleepUntil(limiter.acquire());
            if (System.nanoTime() >= endNanos) {
                break;
            }
            // Bound in-flight establishes; failing to get a permit means the system can't keep up at
            // this rate (the latency knee) — count it and skip so achieved-rate reflects reality.
            if (!inFlight.tryAcquire()) {
                fellBehind++;
                continue;
            }
            long id = pool.nextId();
            long opStart = System.nanoTime();
            pool.establish(id)
                    .whenComplete(
                            (h, ex) -> {
                                inFlight.release();
                                if (ex == null) {
                                    created.increment();
                                    establish.recordValue((System.nanoTime() - opStart) / 1_000_000.0);
                                    synchronized (order) {
                                        order.add(id);
                                    }
                                } else {
                                    failed.increment();
                                }
                            });
            Long old;
            synchronized (order) {
                old = order.poll();
            }
            if (old != null) {
                departed.increment();
                if (graceful) {
                    pool.close(old);
                } else {
                    pool.kill(old);
                }
            }
        }
        // Let in-flight establishes drain so their latencies land in the histogram.
        try {
            inFlight.tryAcquire(concurrency, timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        double seconds = (System.nanoTime() - startNanos) / 1_000_000_000.0;
        DoubleHistogram h = establish.getIntervalHistogram();
        ChurnPoint p = new ChurnPoint();
        p.targetRate = rate;
        p.established = created.sum();
        p.establishFailed = failed.sum();
        p.fellBehind = fellBehind;
        p.achievedCreateRate = seconds > 0 ? created.sum() / seconds : 0;
        p.achievedDepartRate = seconds > 0 ? departed.sum() / seconds : 0;
        p.establishP50 = h.getTotalCount() > 0 ? h.getValueAtPercentile(50) : 0;
        p.establishP99 = h.getTotalCount() > 0 ? h.getValueAtPercentile(99) : 0;
        p.establishMax = h.getTotalCount() > 0 ? h.getMaxValue() : 0;
        p.sustained = created.sum() >= 0.95 * rate * seconds;
        log.info()
                .attr("targetRate", String.format("%.0f", rate))
                .attr("achieved", String.format("%.0f", p.achievedCreateRate))
                .attr("establishP99ms", String.format("%.1f", p.establishP99))
                .attr("sustained", p.sustained)
                .log("S2 churn rate point");
        return p;
    }

    // ---- small helpers --------------------------------------------------------------------------

    private static void sleep(Duration d) {
        long ms = d.toMillis();
        if (ms <= 0) {
            return;
        }
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
