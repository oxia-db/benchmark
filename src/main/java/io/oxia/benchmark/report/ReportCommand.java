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
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.CustomLog;
import org.HdrHistogram.DoubleHistogram;
import org.HdrHistogram.DoubleHistogramIterationValue;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Aggregates the per-worker, per-workload result files produced by a benchmark run (one JSONL line
 * each) into a single per-workload summary, merging the HdrHistograms across workers for exact
 * aggregated percentiles. Emits raw data (summary.csv, summary.json) plus a self-contained
 * report.html chart.
 */
@CustomLog
@Command(
        name = "report",
        description = "Aggregate per-workload result files into CSV, JSON and an HTML chart")
public class ReportCommand implements Callable<Integer> {

    private static final ObjectMapper JSON = new ObjectMapper();

    @Option(
            names = "--results-dir",
            required = true,
            description = "Directory of per-worker *.jsonl result files")
    private Path resultsDir;

    @Option(
            names = {"-o", "--out"},
            defaultValue = ".",
            description = "Output directory for summary.csv / summary.json / report.html")
    private Path outDir;

    @Override
    public Integer call() throws Exception {
        List<WorkloadResult> all = readResults(resultsDir);
        if (all.isEmpty()) {
            log.errorf("No result lines found under %s", resultsDir);
            return 1;
        }

        // Group by workload, then by driver, so each backend is aggregated separately for comparison.
        Map<Integer, Map<String, List<WorkloadResult>>> byWorkloadDriver =
                all.stream()
                        .collect(
                                Collectors.groupingBy(
                                        r -> r.index,
                                        TreeMap::new,
                                        Collectors.groupingBy(r -> r.driver, TreeMap::new, Collectors.toList())));

        Files.createDirectories(outDir);
        List<Summary> summaries = new ArrayList<>();
        for (Map<String, List<WorkloadResult>> perDriver : byWorkloadDriver.values()) {
            for (List<WorkloadResult> group : perDriver.values()) {
                DoubleHistogram writeHist = merge(group.stream().map(r -> r.writeHistB64).toList());
                DoubleHistogram readHist = merge(group.stream().map(r -> r.readHistB64).toList());
                Summary s = summarize(group, writeHist, readHist);
                summaries.add(s);
                writeHgrm(s.index, s.driver, "write", writeHist);
                writeHgrm(s.index, s.driver, "read", readHist);
            }
        }

        writeJson(summaries);
        writeCsv(summaries);
        writeHtml(summaries);

        log.infof(
                "Aggregated %d (workload, driver) summaries from %d worker result(s) into %s",
                summaries.size(), all.size(), outDir.toAbsolutePath());
        return 0;
    }

    private static List<WorkloadResult> readResults(Path dir) throws IOException {
        List<WorkloadResult> out = new ArrayList<>();
        try (Stream<Path> files = Files.list(dir)) {
            for (Path p : files.filter(f -> f.toString().endsWith(".jsonl")).toList()) {
                for (String line : Files.readAllLines(p)) {
                    if (!line.isBlank()) {
                        out.add(JSON.readValue(line, WorkloadResult.class));
                    }
                }
            }
        }
        return out;
    }

    private static Summary summarize(
            List<WorkloadResult> group, DoubleHistogram writeHist, DoubleHistogram readHist) {
        WorkloadResult first = group.get(0);
        Summary s = new Summary();
        s.index = first.index;
        s.driver = first.driver;
        s.workers = group.size();
        s.readRatio = first.readRatio;
        s.keyspaceSize = first.keyspaceSize;
        s.keyDistribution = first.keyDistribution;
        s.valueSize = first.valueSize;
        s.targetRate = first.targetRate;
        s.parallelism = first.parallelism;
        s.duration = first.duration;
        s.measuredSeconds = group.stream().mapToDouble(r -> r.measuredSeconds).max().orElse(0);
        s.failed = group.stream().mapToLong(r -> r.failedCount).sum();

        fill(s.write, writeHist, s.measuredSeconds);
        fill(s.read, readHist, s.measuredSeconds);
        return s;
    }

    private static DoubleHistogram merge(List<String> blobs) {
        DoubleHistogram agg = null;
        for (String b : blobs) {
            if (b == null || b.isEmpty()) {
                continue;
            }
            DoubleHistogram h = HistogramCodec.decode(b);
            if (agg == null) {
                agg = h;
            } else {
                agg.add(h);
            }
        }
        return agg != null ? agg : new DoubleHistogram(3);
    }

    private static void fill(Stats s, DoubleHistogram h, double seconds) {
        s.count = h.getTotalCount();
        s.opsPerSec = seconds > 0 ? s.count / seconds : 0;
        if (s.count > 0) {
            s.p50 = h.getValueAtPercentile(50);
            s.p95 = h.getValueAtPercentile(95);
            s.p99 = h.getValueAtPercentile(99);
            s.p999 = h.getValueAtPercentile(99.9);
            s.max = h.getMaxValue();
            // Percentile sweep for the HdrHistogram-style distribution chart: [percentile, ms].
            for (DoubleHistogramIterationValue v : h.percentiles(5)) {
                double p = v.getPercentileLevelIteratedTo();
                if (p < 100.0) {
                    s.dist.add(new double[] {p, v.getValueIteratedTo()});
                }
            }
        }
    }

    private void writeJson(List<Summary> summaries) throws IOException {
        JSON.writerWithDefaultPrettyPrinter()
                .writeValue(outDir.resolve("summary.json").toFile(), summaries);
    }

    private void writeCsv(List<Summary> summaries) throws IOException {
        StringBuilder sb = new StringBuilder();
        sb.append(
                "index,driver,workers,readRatio,keyspaceSize,keyDistribution,valueSize,targetRate,"
                        + "parallelism,measuredSeconds,failed,write_ops_s,write_p50_ms,write_p95_ms,write_p99_ms,"
                        + "write_p999_ms,write_max_ms,read_ops_s,read_p50_ms,read_p95_ms,read_p99_ms,"
                        + "read_p999_ms,read_max_ms\n");
        for (Summary s : summaries) {
            sb.append(
                    String.format(
                            Locale.ROOT,
                            "%d,%s,%d,%.2f,%d,%s,%d,%.0f,%d,%.1f,%d,"
                                    + "%.1f,%.3f,%.3f,%.3f,%.3f,%.3f,"
                                    + "%.1f,%.3f,%.3f,%.3f,%.3f,%.3f%n",
                            s.index,
                            s.driver,
                            s.workers,
                            s.readRatio,
                            s.keyspaceSize,
                            s.keyDistribution,
                            s.valueSize,
                            s.targetRate,
                            s.parallelism,
                            s.measuredSeconds,
                            s.failed,
                            s.write.opsPerSec,
                            s.write.p50,
                            s.write.p95,
                            s.write.p99,
                            s.write.p999,
                            s.write.max,
                            s.read.opsPerSec,
                            s.read.p50,
                            s.read.p95,
                            s.read.p99,
                            s.read.p999,
                            s.read.max));
        }
        Files.writeString(outDir.resolve("summary.csv"), sb.toString());
    }

    private void writeHtml(List<Summary> summaries) throws IOException {
        String template;
        try (InputStream in = getClass().getResourceAsStream("/report-template.html")) {
            if (in == null) {
                throw new IOException("report-template.html missing from classpath");
            }
            template = new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
        String data = JSON.writeValueAsString(summaries);
        Files.writeString(outDir.resolve("report.html"), template.replace("/*__DATA__*/", data));
    }

    private void writeHgrm(int index, String driver, String type, DoubleHistogram h)
            throws IOException {
        if (h.getTotalCount() == 0) {
            return;
        }
        Path file = outDir.resolve("workload-" + index + "-" + driver + "-" + type + ".hgrm");
        try (PrintStream ps =
                new PrintStream(Files.newOutputStream(file), false, StandardCharsets.UTF_8)) {
            // Official HdrHistogram percentile-distribution format (ms); plot at
            // https://hdrhistogram.github.io/HdrHistogram/
            h.outputPercentileDistribution(ps, 1.0);
        }
    }

    /** Per-operation-type aggregated stats (latencies in ms). */
    public static class Stats {
        public double opsPerSec;
        public long count;
        public double p50;
        public double p95;
        public double p99;
        public double p999;
        public double max;
        public List<double[]> dist = new ArrayList<>();
    }

    /** Aggregated metrics for one workload across all workers. */
    public static class Summary {
        public int index;
        public String driver;
        public int workers;
        public double readRatio;
        public long keyspaceSize;
        public String keyDistribution;
        public int valueSize;
        public double targetRate;
        public int parallelism;
        public String duration;
        public double measuredSeconds;
        public long failed;
        public Stats write = new Stats();
        public Stats read = new Stats();
    }
}
