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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.nio.file.Path;
import org.HdrHistogram.DoubleHistogram;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

class ReportTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    @Test
    void codecRoundTrips() {
        DoubleHistogram h = new DoubleHistogram(3);
        for (int i = 1; i <= 1000; i++) {
            h.recordValue(i);
        }
        DoubleHistogram back = HistogramCodec.decode(HistogramCodec.encode(h));
        assertThat(back.getTotalCount()).isEqualTo(1000);
        assertThat(back.getValueAtPercentile(99)).isEqualTo(h.getValueAtPercentile(99));
    }

    @Test
    void mergesWorkersExactlyAndWritesOutputs(@TempDir Path tmp) throws Exception {
        Path results = tmp.resolve("results");
        Files.createDirectories(results);
        writeWorker(results, "w1", "oxia", 2.0); // 10k ops @ 2ms
        writeWorker(results, "w2", "oxia", 20.0); // 10k ops @ 20ms

        Path out = tmp.resolve("out");
        int code =
                new CommandLine(new ReportCommand())
                        .execute("--results-dir", results.toString(), "-o", out.toString());

        assertThat(code).isZero();
        assertThat(out.resolve("summary.csv")).exists();
        assertThat(out.resolve("report.html")).exists();

        JsonNode arr = JSON.readTree(out.resolve("summary.json").toFile());
        assertThat(arr).hasSize(1); // one driver, one workload
        JsonNode write = arr.get(0).get("write");
        assertThat(arr.get(0).get("workers").asInt()).isEqualTo(2);
        assertThat(write.get("count").asLong()).isEqualTo(20_000);
        // Exact merge of half@2ms + half@20ms: median lands at 2ms, the tail at 20ms.
        assertThat(write.get("p50").asDouble()).isCloseTo(2.0, within(0.1));
        assertThat(write.get("p99").asDouble()).isCloseTo(20.0, within(0.1));
        assertThat(write.get("max").asDouble()).isCloseTo(20.0, within(0.1));
        assertThat(write.get("opsPerSec").asDouble()).isCloseTo(2000.0, within(1.0));
        assertThat(write.get("dist").size()).isGreaterThan(2);
        assertThat(out.resolve("workload-0-oxia-write.hgrm")).exists();
        assertThat(out.resolve("workload-0-oxia-read.hgrm")).doesNotExist();

        // name + description flow from config through to the aggregated outputs
        assertThat(arr.get(0).get("name").asText()).isEqualTo("write-only");
        assertThat(arr.get(0).get("description").asText()).isEqualTo("Insert phase, 100% writes");
        String csv = Files.readString(out.resolve("summary.csv"));
        assertThat(csv).contains("index,name,description,driver");
        assertThat(csv).contains("\"Insert phase, 100% writes\""); // comma-containing field gets quoted
    }

    @Test
    void comparesAcrossDrivers(@TempDir Path tmp) throws Exception {
        Path results = tmp.resolve("results");
        Files.createDirectories(results);
        writeWorker(results, "oxia-w1", "oxia", 3.0);
        writeWorker(results, "etcd-w1", "etcd", 9.0);

        Path out = tmp.resolve("out");
        int code =
                new CommandLine(new ReportCommand())
                        .execute("--results-dir", results.toString(), "-o", out.toString());
        assertThat(code).isZero();

        // Same workload index, two drivers -> two separate summaries, each with its own latency.
        JsonNode arr = JSON.readTree(out.resolve("summary.json").toFile());
        assertThat(arr).hasSize(2);
        for (JsonNode s : arr) {
            assertThat(s.get("index").asInt()).isZero();
            double p50 = s.get("write").get("p50").asDouble();
            if (s.get("driver").asText().equals("oxia")) {
                assertThat(p50).isCloseTo(3.0, within(0.1));
            } else {
                assertThat(p50).isCloseTo(9.0, within(0.1));
            }
        }
        assertThat(out.resolve("workload-0-oxia-write.hgrm")).exists();
        assertThat(out.resolve("workload-0-etcd-write.hgrm")).exists();
    }

    private void writeWorker(Path dir, String id, String driver, double latencyMs) throws Exception {
        DoubleHistogram h = new DoubleHistogram(3);
        for (int i = 0; i < 10_000; i++) {
            h.recordValue(latencyMs);
        }
        WorkloadResult r = new WorkloadResult();
        r.index = 0;
        r.instanceId = id;
        r.name = "write-only";
        r.description = "Insert phase, 100% writes";
        r.driver = driver;
        r.keyDistribution = "order";
        r.keyspaceSize = 1000;
        r.valueSize = 64;
        r.targetRate = 10_000;
        r.parallelism = 1;
        r.duration = "PT10S";
        r.measuredSeconds = 10.0;
        r.writeCount = h.getTotalCount();
        r.writeHistB64 = HistogramCodec.encode(h);
        r.readHistB64 = HistogramCodec.encode(new DoubleHistogram(3));
        Files.writeString(dir.resolve(id + ".jsonl"), JSON.writeValueAsString(r) + "\n");
    }
}
