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

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.oxia.benchmark.driver.KVStoreDriver;
import io.oxia.benchmark.report.HistogramCodec;
import io.oxia.benchmark.report.WorkloadResult;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.LongAdder;
import org.HdrHistogram.DoubleHistogram;
import org.junit.jupiter.api.Test;

class BenchmarkRunnerTest {

    // Regression for the histogram reset/record race. With the old ConcurrentDoubleHistogram +
    // reset(), a worker's recordValue() landing mid-reset corrupted the internal value-scaling
    // ratio and produced absurd latencies (~1e242 ms) plus silently dropped later samples. The
    // stats-interval swap is the exact site the user hit; we drive it every 20ms here (vs the 10s
    // default) so 8 threads recording flat out race many swaps. With the DoubleRecorder
    // interval-swap the reported latencies must stay physically sane on every run.
    @Test
    void reportedLatenciesStayBoundedAcrossManyStatsIntervals() throws Exception {
        String prev = System.setProperty("benchmark.statsIntervalMs", "20");
        try {
            MockDriver driver = new MockDriver();
            ObjectMapper mapper = new ObjectMapper();
            Map<String, Object> map = new LinkedHashMap<>();
            map.put("readRatio", 0.0);
            map.put("keyspaceSize", 1000);
            map.put("keyDistribution", "uniform");
            map.put("valueSize", 8);
            map.put("targetRate", 0.0); // throughput mode: record as fast as possible
            map.put("warmup", "50ms"); // also exercise the warmup-boundary interval swap under load
            map.put("duration", "1s"); // ~50 stats-interval swaps while workers hammer the recorder
            map.put("parallelism", 8);
            Workload wl = mapper.convertValue(map, Workload.class);

            WorkloadResult r = new BenchmarkRunner(wl, driver).run();

            DoubleHistogram writeHist = HistogramCodec.decode(r.writeHistB64);
            assertThat(writeHist.getTotalCount()).isGreaterThan(0);
            // Mock ops complete inline (sub-ms); a corrupted histogram reports astronomically large
            // values, so any sane cap well above real latency but far below corruption catches it.
            assertThat(writeHist.getMaxValue()).isLessThan(1000.0);
            assertThat(writeHist.getValueAtPercentile(99.9)).isLessThan(1000.0);
        } finally {
            if (prev == null) {
                System.clearProperty("benchmark.statsIntervalMs");
            } else {
                System.setProperty("benchmark.statsIntervalMs", prev);
            }
        }
    }

    @Test
    void writeOnlyWorkload() throws Exception {
        MockDriver driver = new MockDriver();
        Workload wl = createWorkload(0.0, "order");

        new BenchmarkRunner(wl, driver).run();

        assertThat(driver.putCount.sum()).isGreaterThan(0);
        assertThat(driver.getCount.sum()).isEqualTo(0);
    }

    @Test
    void readOnlyWorkload() throws Exception {
        MockDriver driver = new MockDriver();
        Workload wl = createWorkload(1.0, "uniform");

        new BenchmarkRunner(wl, driver).run();

        assertThat(driver.getCount.sum()).isGreaterThan(0);
        assertThat(driver.putCount.sum()).isEqualTo(0);
    }

    @Test
    void mixedWorkload() throws Exception {
        MockDriver driver = new MockDriver();
        Workload wl = createWorkload(0.5, "zipf");

        new BenchmarkRunner(wl, driver).run();

        assertThat(driver.putCount.sum()).isGreaterThan(0);
        assertThat(driver.getCount.sum()).isGreaterThan(0);
    }

    private static Workload createWorkload(double readRatio, String distribution) {
        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("readRatio", readRatio);
        map.put("keyspaceSize", 1000);
        map.put("keyDistribution", distribution);
        map.put("valueSize", 64);
        map.put("targetRate", 10000.0);
        map.put("duration", "2s");
        map.put("parallelism", 2);
        return mapper.convertValue(map, Workload.class);
    }

    static class MockDriver implements KVStoreDriver {
        final LongAdder putCount = new LongAdder();
        final LongAdder getCount = new LongAdder();

        @Override
        public String name() {
            return "mock";
        }

        @Override
        public void init(Map<String, Object> config) {}

        @Override
        public CompletableFuture<Void> put(String key, byte[] value) {
            putCount.increment();
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> get(String key) {
            getCount.increment();
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void close() {}
    }
}
