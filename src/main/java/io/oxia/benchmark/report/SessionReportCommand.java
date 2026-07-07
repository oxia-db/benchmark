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
package io.oxia.benchmark.report;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.oxia.benchmark.session.SessionResult;
import io.oxia.benchmark.session.SessionResult.CapacityPoint;
import io.oxia.benchmark.session.SessionResult.ChurnPoint;
import io.oxia.benchmark.session.SessionResult.CleanupTrial;
import io.oxia.benchmark.session.SessionResult.StormRun;
import io.oxia.benchmark.session.SessionResult.StormSample;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.stream.Stream;
import lombok.CustomLog;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Aggregates the per-worker session-experiment result files (one JSONL line each, {@link
 * SessionResult}) into chart-ready CSV plus a raw JSON dump — the session-suite analogue of {@code
 * ReportCommand}. Each experiment type flattens to its own CSV: time series for S1/S2/S4 and one
 * row per trial for S3 (so a CDF plots directly). S1 and S2 also get a headline CSV with the
 * deliverable numbers (max sustainable N under 5% degradation; max sustainable churn). Rows keep
 * {@code instanceId} so nothing is lost to aggregation; per-cluster roll-ups sum foreground
 * throughput across workers and average per-worker footprint.
 */
@CustomLog
@Command(
        name = "session-report",
        description = "Aggregate session-experiment result files into per-experiment CSV and JSON")
public class SessionReportCommand implements Callable<Integer> {

    private static final ObjectMapper JSON = new ObjectMapper();

    @Option(
            names = "--results-dir",
            required = true,
            description = "Directory of per-worker *.jsonl session result files")
    private Path resultsDir;

    @Option(
            names = {"-o", "--out"},
            defaultValue = ".",
            description = "Output directory for the session-*.csv / session-summary.json files")
    private Path outDir;

    @Override
    public Integer call() throws Exception {
        List<SessionResult> all = readResults(resultsDir);
        if (all.isEmpty()) {
            log.errorf("No session result lines found under %s", resultsDir);
            return 1;
        }
        Files.createDirectories(outDir);

        writeCapacity(all.stream().filter(r -> "S1".equals(r.type)).toList());
        writeChurn(all.stream().filter(r -> "S2".equals(r.type)).toList());
        writeCleanup(all.stream().filter(r -> "S3".equals(r.type)).toList());
        writeStorm(all.stream().filter(r -> "S4".equals(r.type)).toList());

        JSON.writerWithDefaultPrettyPrinter()
                .writeValue(outDir.resolve("session-summary.json").toFile(), all);

        log.infof("Aggregated %d session result(s) into %s", all.size(), outDir.toAbsolutePath());
        return 0;
    }

    private static List<SessionResult> readResults(Path dir) throws IOException {
        List<SessionResult> out = new ArrayList<>();
        try (Stream<Path> files = Files.list(dir)) {
            for (Path p : files.filter(f -> f.toString().endsWith("-session.jsonl")).toList()) {
                for (String line : Files.readAllLines(p)) {
                    if (!line.isBlank()) {
                        SessionResult r = JSON.readValue(line, SessionResult.class);
                        if (r.type != null) {
                            out.add(r);
                        }
                    }
                }
            }
        }
        return out;
    }

    // ---- S1 capacity ----------------------------------------------------------------------------

    private void writeCapacity(List<SessionResult> results) throws IOException {
        if (results.isEmpty()) {
            return;
        }
        StringBuilder sb = new StringBuilder();
        sb.append(
                "index,name,driver,instanceId,sessions,foreground_throughput,foreground_p50_ms,"
                        + "foreground_p99_ms,foreground_max_ms,foreground_failed,degradation_pct,"
                        + "sockets_per10k,threads_per10k,heap_mb_per10k\n");
        for (SessionResult r : results) {
            for (CapacityPoint p : nn(r.capacity)) {
                sb.append(
                        String.format(
                                Locale.ROOT,
                                "%d,%s,%s,%s,%d,%.1f,%.3f,%.3f,%.3f,%d,%.2f,%d,%d,%.2f%n",
                                r.index,
                                csv(r.name),
                                r.driver,
                                csv(r.instanceId),
                                p.sessions,
                                p.foregroundThroughput,
                                p.foregroundP50,
                                p.foregroundP99,
                                p.foregroundMax,
                                p.foregroundFailed,
                                p.degradationPct,
                                p.footprintSocketsPer10k,
                                p.footprintThreadsPer10k,
                                p.footprintHeapMBPer10k));
            }
        }
        write("session-s1.csv", sb.toString());
        writeCapacityHeadline(results);
    }

    /** Cluster roll-up per (index, driver): throughput summed across workers, headline max N < 5%. */
    private void writeCapacityHeadline(List<SessionResult> results) throws IOException {
        StringBuilder sb = new StringBuilder();
        sb.append(
                "index,name,driver,workers,sessions,cluster_throughput,degradation_pct,"
                        + "mean_threads_per10k,mean_heap_mb_per10k,max_sustainable_n\n");
        for (Map.Entry<String, List<SessionResult>> e : groupByIndexDriver(results).entrySet()) {
            List<SessionResult> group = e.getValue();
            SessionResult first = group.get(0);
            // Aggregate each N across workers.
            Map<Long, double[]> byN =
                    new TreeMap<>(); // N -> [sumThroughput, sumDeg, sumThreads, sumHeap, count]
            for (SessionResult r : group) {
                for (CapacityPoint p : nn(r.capacity)) {
                    double[] acc = byN.computeIfAbsent(p.sessions, k -> new double[5]);
                    acc[0] += p.foregroundThroughput;
                    acc[1] += p.degradationPct;
                    acc[2] += p.footprintThreadsPer10k;
                    acc[3] += p.footprintHeapMBPer10k;
                    acc[4] += 1;
                }
            }
            double baseline =
                    byN.containsKey(0L)
                            ? byN.get(0L)[0]
                            : byN.values().stream().findFirst().orElse(new double[5])[0];
            long maxSustainable = 0;
            for (Map.Entry<Long, double[]> pe : byN.entrySet()) {
                long n = pe.getKey();
                double[] acc = pe.getValue();
                double clusterThroughput = acc[0];
                double degradation = baseline > 0 ? (baseline - clusterThroughput) / baseline * 100.0 : 0.0;
                if (degradation < 5.0 && n > maxSustainable) {
                    maxSustainable = n;
                }
                sb.append(
                        String.format(
                                Locale.ROOT,
                                "%d,%s,%s,%d,%d,%.1f,%.2f,%.1f,%.2f,%d%n",
                                first.index,
                                csv(first.name),
                                first.driver,
                                group.size(),
                                n,
                                clusterThroughput,
                                degradation,
                                acc[4] > 0 ? acc[2] / acc[4] : 0,
                                acc[4] > 0 ? acc[3] / acc[4] : 0,
                                maxSustainable));
            }
            log.info()
                    .attr("experiment", first.name)
                    .attr("driver", first.driver)
                    .attr("maxSustainableN", maxSustainable)
                    .log("S1 headline: max live sessions under 5% foreground degradation");
        }
        write("session-s1-headline.csv", sb.toString());
    }

    // ---- S2 churn -------------------------------------------------------------------------------

    private void writeChurn(List<SessionResult> results) throws IOException {
        if (results.isEmpty()) {
            return;
        }
        StringBuilder sb = new StringBuilder();
        sb.append(
                "index,name,driver,instanceId,departure,target_rate,achieved_create_rate,"
                        + "achieved_depart_rate,established,establish_failed,fell_behind,"
                        + "establish_p50_ms,establish_p99_ms,establish_max_ms,sustained\n");
        for (SessionResult r : results) {
            for (ChurnPoint p : nn(r.churn)) {
                sb.append(
                        String.format(
                                Locale.ROOT,
                                "%d,%s,%s,%s,%s,%.0f,%.1f,%.1f,%d,%d,%d,%.3f,%.3f,%.3f,%b%n",
                                r.index,
                                csv(r.name),
                                r.driver,
                                csv(r.instanceId),
                                r.departure,
                                p.targetRate,
                                p.achievedCreateRate,
                                p.achievedDepartRate,
                                p.established,
                                p.establishFailed,
                                p.fellBehind,
                                p.establishP50,
                                p.establishP99,
                                p.establishMax,
                                p.sustained));
            }
        }
        write("session-s2.csv", sb.toString());
        writeChurnHeadline(results);
    }

    private void writeChurnHeadline(List<SessionResult> results) throws IOException {
        StringBuilder sb = new StringBuilder();
        sb.append(
                "index,name,driver,workers,departure,target_rate,cluster_achieved_rate,"
                        + "all_sustained,max_establish_p99_ms,max_sustainable_churn\n");
        for (Map.Entry<String, List<SessionResult>> e : groupByIndexDriver(results).entrySet()) {
            List<SessionResult> group = e.getValue();
            SessionResult first = group.get(0);
            Map<Double, double[]> byRate =
                    new TreeMap<>(); // rate -> [sumAchieved, maxP99, allSustained(1/0), count]
            for (SessionResult r : group) {
                for (ChurnPoint p : nn(r.churn)) {
                    double[] acc = byRate.computeIfAbsent(p.targetRate, k -> new double[] {0, 0, 1, 0});
                    acc[0] += p.achievedCreateRate;
                    acc[1] = Math.max(acc[1], p.establishP99);
                    acc[2] = acc[2] == 1 && p.sustained ? 1 : 0;
                    acc[3] += 1;
                }
            }
            double maxSustainable = 0;
            for (Map.Entry<Double, double[]> re : byRate.entrySet()) {
                double rate = re.getKey();
                double[] acc = re.getValue();
                boolean allSustained = acc[2] == 1;
                if (allSustained && rate > maxSustainable) {
                    maxSustainable = rate;
                }
                sb.append(
                        String.format(
                                Locale.ROOT,
                                "%d,%s,%s,%d,%s,%.0f,%.1f,%b,%.3f,%.0f%n",
                                first.index,
                                csv(first.name),
                                first.driver,
                                group.size(),
                                first.departure,
                                rate,
                                acc[0],
                                allSustained,
                                acc[1],
                                maxSustainable));
            }
            log.info()
                    .attr("experiment", first.name)
                    .attr("driver", first.driver)
                    .attr("maxSustainableChurn", String.format("%.0f", maxSustainable))
                    .log("S2 headline: max sustainable churn (sessions/s)");
        }
        write("session-s2-headline.csv", sb.toString());
    }

    // ---- S3 cleanup-visibility (one row per trial → CDF) ----------------------------------------

    private void writeCleanup(List<SessionResult> results) throws IOException {
        if (results.isEmpty()) {
            return;
        }
        StringBuilder sb = new StringBuilder();
        sb.append(
                "index,name,driver,instanceId,session_timeout_ms,ephemerals_per_session,load,trial,"
                        + "notified,gone,excess_ms,gone_excess_ms,dispatch_ms\n");
        for (SessionResult r : results) {
            for (CleanupTrial t : nn(r.cleanupTrials)) {
                sb.append(
                        String.format(
                                Locale.ROOT,
                                "%d,%s,%s,%s,%.0f,%d,%s,%d,%b,%b,%.3f,%.3f,%.3f%n",
                                r.index,
                                csv(r.name),
                                r.driver,
                                csv(r.instanceId),
                                r.sessionTimeoutMs,
                                r.ephemeralsPerSession,
                                t.load,
                                t.trial,
                                t.notified,
                                t.gone,
                                t.excessMs,
                                t.goneExcessMs,
                                t.dispatchMs));
            }
        }
        write("session-s3.csv", sb.toString());
    }

    // ---- S4 expiry storm (time series) ----------------------------------------------------------

    private void writeStorm(List<SessionResult> results) throws IOException {
        if (results.isEmpty()) {
            return;
        }
        StringBuilder sb = new StringBuilder();
        sb.append(
                "index,name,driver,instanceId,kill_fraction,killed,sampled_keys,completion_ms,"
                        + "t_ms,fraction_deleted,fg_throughput,fg_p99_ms\n");
        for (SessionResult r : results) {
            for (StormRun run : nn(r.storm)) {
                for (StormSample s : nn(run.timeline)) {
                    sb.append(
                            String.format(
                                    Locale.ROOT,
                                    "%d,%s,%s,%s,%.2f,%d,%d,%.0f,%.1f,%.4f,%.1f,%.3f%n",
                                    r.index,
                                    csv(r.name),
                                    r.driver,
                                    csv(r.instanceId),
                                    run.killFraction,
                                    run.killed,
                                    run.sampledKeys,
                                    run.completionMs,
                                    s.tMs,
                                    s.fractionDeleted,
                                    s.foregroundThroughput,
                                    s.foregroundP99));
                }
            }
        }
        write("session-s4.csv", sb.toString());
    }

    // ---- helpers --------------------------------------------------------------------------------

    private Map<String, List<SessionResult>> groupByIndexDriver(List<SessionResult> results) {
        Map<String, List<SessionResult>> byGroup = new LinkedHashMap<>();
        for (SessionResult r : results) {
            byGroup.computeIfAbsent(r.index + "|" + r.driver, k -> new ArrayList<>()).add(r);
        }
        return byGroup;
    }

    private void write(String file, String content) throws IOException {
        Files.writeString(outDir.resolve(file), content);
    }

    private static <T> List<T> nn(List<T> list) {
        return list == null ? List.of() : list;
    }

    /** RFC-4180 CSV field: quote (and double inner quotes) only when needed. */
    private static String csv(String v) {
        if (v == null) {
            return "";
        }
        if (v.contains(",") || v.contains("\"") || v.contains("\n")) {
            return "\"" + v.replace("\"", "\"\"") + "\"";
        }
        return v;
    }
}
