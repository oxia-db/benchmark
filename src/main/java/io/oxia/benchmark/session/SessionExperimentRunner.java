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

import io.oxia.benchmark.driver.session.PrefixListener;
import io.oxia.benchmark.driver.session.SessionDriver;
import io.oxia.benchmark.session.SessionResult.CapacityPoint;
import io.oxia.benchmark.session.SessionResult.ChurnPoint;
import io.oxia.benchmark.session.SessionResult.CleanupTrial;
import io.oxia.benchmark.session.SessionResult.StormRun;
import io.oxia.benchmark.session.SessionResult.StormSample;
import java.io.Closeable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import lombok.CustomLog;
import org.HdrHistogram.DoubleHistogram;
import org.HdrHistogram.DoubleRecorder;

/**
 * Runs one {@link SessionExperiment} (S1–S4) against a {@link SessionDriver} and returns a {@link
 * SessionResult}. The orchestration runs on this one thread and drives thousands of sessions
 * through the async {@link SessionPool}; the sessions themselves are never thread-backed. Every
 * experiment uses the identical session timeout and ephemeral count from the config, so results are
 * comparable across backends.
 */
@CustomLog
public class SessionExperimentRunner {

    /**
     * Fine cadence for the S3 first-absent-read probe; sub-{@code sampleIntervalMs} for tight t_gone.
     */
    private static final long GONE_POLL_MS = 25;

    private final SessionExperiment exp;
    private final SessionDriver driver;
    private final String instanceId;
    private final SessionKeys keys;
    private final Duration timeout;
    private final Duration settle;
    private final int concurrency;
    private final int k;
    private final int ephemeralValueSize;
    private ScheduledExecutorService scheduler;

    public SessionExperimentRunner(SessionExperiment exp, SessionDriver driver, String instanceId) {
        this.exp = exp;
        this.driver = driver;
        this.instanceId = instanceId == null ? "local" : instanceId;
        // Namespace keys per worker so multiple workers against one cluster never collide.
        this.keys = new SessionKeys(exp.keyPrefix() + "/" + this.instanceId);
        this.timeout = exp.sessionTimeout();
        this.settle = exp.warmup().isZero() ? Duration.ofSeconds(3) : exp.warmup();
        this.concurrency = exp.createConcurrency();
        this.k = exp.ephemeralsPerSession();
        this.ephemeralValueSize = exp.ephemeralValueSize();
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
        r.ephemeralsPerSession = k;

        scheduler =
                Executors.newScheduledThreadPool(
                        2,
                        runnable -> {
                            Thread t = new Thread(runnable, "session-probe");
                            t.setDaemon(true);
                            return t;
                        });
        try {
            switch (exp.type()) {
                case "S1" -> runCapacity(r);
                case "S2" -> runChurn(r);
                case "S3" -> runCleanup(r);
                case "S4" -> runStorm(r);
                default -> throw new IllegalArgumentException("unknown experiment type: " + exp.type());
            }
        } finally {
            scheduler.shutdownNow();
        }
        return r;
    }

    private SessionPool newPool() {
        return new SessionPool(driver, keys, k, timeout, ephemeralValueSize);
    }

    private ForegroundLoad newForeground() {
        return new ForegroundLoad(
                driver,
                exp.foregroundRate(),
                exp.foregroundReadRatio(),
                exp.foregroundKeyspaceSize(),
                exp.foregroundKeyDistribution(),
                exp.foregroundValueSize(),
                exp.foregroundParallelism(),
                10_000);
    }

    // ---- S1 capacity ----------------------------------------------------------------------------

    private void runCapacity(SessionResult r) {
        r.capacity = new ArrayList<>();
        SessionPool pool = newPool();
        ForegroundLoad fg = newForeground();
        fg.start();
        sleep(exp.warmup());

        // Baseline: foreground alone (N=0). This anchors both throughput degradation and footprint.
        Footprint baseFp = FootprintSampler.sample();
        fg.snapshotInterval(); // reset window
        sleep(exp.holdDuration());
        ForegroundLoad.IntervalStats base = fg.snapshotInterval();
        double baseline = base.throughput();
        r.capacity.add(capacityPoint(0, base, 0.0, baseFp.minus(baseFp).per10k(1)));
        log.info()
                .attr("baselineThroughput", String.format("%.0f", baseline))
                .attr("baselineP99", String.format("%.1f", base.p99()))
                .log("S1 baseline (N=0) measured");

        for (long n : exp.sessionsSweep()) {
            join(pool.rampTo(n, concurrency));
            sleep(settle);
            fg.snapshotInterval(); // reset window
            sleep(exp.holdDuration());
            ForegroundLoad.IntervalStats s = fg.snapshotInterval();
            Footprint delta = FootprintSampler.sample().minus(baseFp);
            double degradation = baseline > 0 ? (baseline - s.throughput()) / baseline * 100.0 : 0.0;
            r.capacity.add(capacityPoint(n, s, degradation, delta.per10k(n)));
            log.info()
                    .attr("N", n)
                    .attr("throughput", String.format("%.0f", s.throughput()))
                    .attr("p99ms", String.format("%.1f", s.p99()))
                    .attr("degradationPct", String.format("%.1f", degradation))
                    .attr("threadsPer10k", delta.per10k(n).threads())
                    .attr("heapMBPer10k", String.format("%.1f", delta.per10k(n).heapMB()))
                    .log("S1 sweep point");
        }
        fg.stop();
        join(pool.closeAll(concurrency));
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
        // One pool for the whole sweep so session ids (and thus ephemeral keys) stay globally unique:
        // a fresh pool per rate would reset the id counter and could collide with keys still expiring
        // from a previous rate's abandoned sessions. Reset the population between rates via closeAll.
        SessionPool pool = newPool();
        for (double rate : exp.churnRateSweep()) {
            r.churn.add(runChurnRate(pool, rate, graceful));
            join(pool.closeAll(concurrency));
        }
        fg.stop();
    }

    private ChurnPoint runChurnRate(SessionPool pool, double rate, boolean graceful) {
        Semaphore inFlight = new Semaphore(concurrency);
        java.util.ArrayDeque<Long> order = new java.util.ArrayDeque<>();
        DoubleRecorder establish = new DoubleRecorder(3);
        LongAdder created = new LongAdder();
        LongAdder failed = new LongAdder();
        LongAdder departed = new LongAdder();
        AtomicLong fellBehind = new AtomicLong();

        io.oxia.benchmark.runner.UniformRateLimiter limiter =
                new io.oxia.benchmark.runner.UniformRateLimiter(rate);
        long startNanos = System.nanoTime();
        long endNanos = startNanos + exp.holdDuration().toNanos();

        while (System.nanoTime() < endNanos) {
            long intended = limiter.acquire();
            io.oxia.benchmark.runner.UniformRateLimiter.sleepUntil(intended);
            if (System.nanoTime() >= endNanos) {
                break;
            }
            // Bound in-flight establishes; failing to get a permit means the system can't keep up at
            // this rate (the latency knee) — count it and skip so achieved-rate reflects reality.
            if (!inFlight.tryAcquire()) {
                fellBehind.incrementAndGet();
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
            inFlight.tryAcquire(
                    concurrency, timeout.toMillis() + settle.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        double seconds = (System.nanoTime() - startNanos) / 1_000_000_000.0;
        DoubleHistogram h = establish.getIntervalHistogram();
        ChurnPoint p = new ChurnPoint();
        p.targetRate = rate;
        p.established = created.sum();
        p.establishFailed = failed.sum();
        p.fellBehind = fellBehind.get();
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

    // ---- S3 cleanup-visibility ------------------------------------------------------------------

    private void runCleanup(SessionResult r) {
        r.cleanupTrials = new ArrayList<>();
        SessionPool pool = newPool();
        join(pool.rampTo(exp.backgroundSessions(), concurrency));
        log.info().attr("background", pool.size()).log("S3 background pool live; running idle trials");
        runCleanupPhase(r, pool, "idle");

        ForegroundLoad fg = newForeground();
        if (fg.active()) {
            fg.start();
            sleep(exp.warmup());
            log.info().log("S3 running trials under foreground load");
            runCleanupPhase(r, pool, "load");
            fg.stop();
        }
        join(pool.closeAll(concurrency));
    }

    private void runCleanupPhase(SessionResult r, SessionPool pool, String load) {
        long budgetNanos = timeout.toNanos() + exp.settleTimeout().toNanos();
        for (int trial = 0; trial < exp.trials(); trial++) {
            long id = pool.nextId();
            join(pool.establish(id)); // target session live with k ephemerals

            // Arm the native change feed on the target's parent, capturing the first deletion time.
            CompletableFuture<Long> notify = new CompletableFuture<>();
            Closeable watch =
                    driver.watchPrefix(
                            keys.parent(id),
                            new PrefixListener() {
                                @Override
                                public void onKeyDeleted(String key, long atNanos) {
                                    notify.complete(atNanos);
                                }
                            });
            sleep(Duration.ofMillis(200)); // let the watch arm and heartbeats settle

            String probeKey = keys.ephemeral(id, 0);
            long tHb = System.nanoTime(); // last heartbeat instant (upper bound; see README)
            join(pool.kill(id)); // abrupt: heartbeats stop now, no goodbye

            long deadline = tHb + budgetNanos;
            CompletableFuture<Long> gone = pollGone(probeKey, deadline);
            Long tGone = await(gone, deadline);
            Long tNotify = await(notify, deadline);
            closeQuietly(watch);

            long contractDeadline = tHb + timeout.toNanos();
            CleanupTrial ct = new CleanupTrial();
            ct.trial = trial;
            ct.load = load;
            if (tNotify != null) {
                ct.notified = true;
                ct.excessMs = (tNotify - contractDeadline) / 1_000_000.0;
            }
            if (tGone != null) {
                ct.gone = true;
                ct.goneExcessMs = (tGone - contractDeadline) / 1_000_000.0;
            }
            if (tNotify != null && tGone != null) {
                ct.dispatchMs = (tNotify - tGone) / 1_000_000.0;
            }
            r.cleanupTrials.add(ct);
            if ((trial + 1) % 25 == 0) {
                log.info()
                        .attr("load", load)
                        .attr("trial", trial + 1)
                        .attr("excessMs", String.format("%.1f", ct.excessMs))
                        .log("S3 trials progressing");
            }
        }
    }

    /**
     * Poll a key until a read returns absent, or the deadline passes; completes with the nano time.
     */
    private CompletableFuture<Long> pollGone(String key, long deadlineNanos) {
        CompletableFuture<Long> result = new CompletableFuture<>();
        scheduleGoneCheck(key, deadlineNanos, result);
        return result;
    }

    private void scheduleGoneCheck(String key, long deadlineNanos, CompletableFuture<Long> result) {
        if (result.isDone()) {
            return;
        }
        if (System.nanoTime() >= deadlineNanos) {
            result.complete(null);
            return;
        }
        driver
                .exists(key)
                .whenComplete(
                        (present, ex) -> {
                            if (result.isDone()) {
                                return;
                            }
                            if (ex == null && !present) {
                                result.complete(System.nanoTime());
                            } else {
                                scheduler.schedule(
                                        () -> scheduleGoneCheck(key, deadlineNanos, result),
                                        GONE_POLL_MS,
                                        TimeUnit.MILLISECONDS);
                            }
                        });
    }

    // ---- S4 expiry storm ------------------------------------------------------------------------

    private void runStorm(SessionResult r) {
        r.storm = new ArrayList<>();
        long n = exp.sessions();
        SessionPool pool = newPool();
        ForegroundLoad fg = newForeground();
        fg.start();
        sleep(exp.warmup());

        for (double fraction : exp.killFractionSweep()) {
            join(pool.rampTo(n, concurrency)); // refill after the previous fraction's kills
            sleep(settle);

            List<Long> liveIds = pool.liveIds();
            int killCount = (int) Math.min(liveIds.size(), Math.round(fraction * n));
            List<Long> killIds = new ArrayList<>(liveIds.subList(0, killCount));
            List<String> sampled = sampleProbeKeys(killIds, exp.sampleKeys());

            StormRun sr = new StormRun();
            sr.killFraction = fraction;
            sr.killed = killIds.size();
            sr.sampledKeys = sampled.size();
            sr.completionMs = Double.NaN;

            fg.snapshotInterval(); // reset just before the storm
            long t0 = System.nanoTime();
            CompletableFuture<Void> killDone = pool.killIds(killIds, concurrency);
            log.info()
                    .attr("killFraction", fraction)
                    .attr("killed", killIds.size())
                    .log("S4 simultaneous kill issued; sampling cleanup curve");

            long deadline = t0 + timeout.toNanos() + exp.settleTimeout().toNanos();
            while (System.nanoTime() < deadline) {
                sleepMs(exp.sampleIntervalMs());
                double fractionDeleted = countAbsent(sampled) / (double) Math.max(1, sampled.size());
                ForegroundLoad.IntervalStats s = fg.snapshotInterval();
                StormSample ss = new StormSample();
                ss.tMs = (System.nanoTime() - t0) / 1_000_000.0;
                ss.fractionDeleted = fractionDeleted;
                ss.foregroundThroughput = s.throughput();
                ss.foregroundP99 = s.p99();
                sr.timeline.add(ss);
                if (fractionDeleted >= 0.999) {
                    sr.completionMs = ss.tMs;
                    break;
                }
            }
            join(killDone);
            r.storm.add(sr);
            log.info()
                    .attr("killFraction", fraction)
                    .attr("completionMs", String.format("%.0f", sr.completionMs))
                    .log("S4 fraction complete");
        }
        fg.stop();
        join(pool.closeAll(concurrency));
    }

    /** Count how many of the sampled keys currently read as absent (bounded-concurrency probe). */
    private int countAbsent(List<String> sampled) {
        AtomicInteger absent = new AtomicInteger();
        join(
                AsyncBatch.run(
                        sampled,
                        concurrency,
                        key ->
                                driver
                                        .exists(key)
                                        .thenAccept(
                                                present -> {
                                                    if (!present) {
                                                        absent.incrementAndGet();
                                                    }
                                                }),
                        (key, ex) -> {}));
        return absent.get();
    }

    /** Representative probe key (e0) for up to {@code max} of the killed sessions, evenly strided. */
    private List<String> sampleProbeKeys(List<Long> killIds, int max) {
        List<String> out = new ArrayList<>(Math.min(max, killIds.size()));
        if (killIds.size() <= max) {
            for (long id : killIds) {
                out.add(keys.ephemeral(id, 0));
            }
        } else {
            double stride = killIds.size() / (double) max;
            for (int i = 0; i < max; i++) {
                out.add(keys.ephemeral(killIds.get((int) (i * stride)), 0));
            }
        }
        return out;
    }

    // ---- small helpers --------------------------------------------------------------------------

    private static void sleep(Duration d) {
        sleepMs(d.toMillis());
    }

    private static void sleepMs(long ms) {
        if (ms <= 0) {
            return;
        }
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static <T> T join(CompletableFuture<T> f) {
        return f.join();
    }

    /** Await a future's value up to the deadline, returning null on timeout/failure. */
    private static Long await(CompletableFuture<Long> f, long deadlineNanos) {
        long remainingMs = Math.max(0, (deadlineNanos - System.nanoTime()) / 1_000_000) + 100;
        try {
            return f.get(remainingMs, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            return null;
        } catch (Exception e) {
            return null;
        }
    }

    private static void closeQuietly(Closeable c) {
        try {
            c.close();
        } catch (Exception ignored) {
            // best-effort watch teardown
        }
    }
}
