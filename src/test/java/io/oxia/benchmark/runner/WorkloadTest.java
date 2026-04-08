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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class WorkloadTest {

    @Test
    void loadValidWorkloads() throws IOException {
        Workloads wls = Workloads.load(Path.of("conf/workload-mixed.yaml"));
        assertThat(wls.items()).hasSize(4);
        assertThat(wls.exitWhenFinish()).isFalse();
        assertThat(wls.metadata().serverNum()).isEqualTo(1);
    }

    @Test
    void firstWorkloadIsInsertOnly() throws IOException {
        Workloads wls = Workloads.load(Path.of("conf/workload-mixed.yaml"));
        Workload first = wls.items().get(0);
        assertThat(first.readRatio()).isEqualTo(0.0);
        assertThat(first.keyspaceSize()).isEqualTo(100_000);
        assertThat(first.keyDistribution()).isEqualTo("order");
        assertThat(first.valueSize()).isEqualTo(64);
        assertThat(first.targetRate()).isEqualTo(40_000);
        assertThat(first.parallelism()).isEqualTo(1);
    }

    @Test
    void missingFileThrows() {
        assertThatThrownBy(() -> Workloads.load(Path.of("nonexistent.yaml")))
                .isInstanceOf(IOException.class);
    }

    @Test
    void invalidKeyspaceSize() {
        assertThatThrownBy(
                        () -> {
                            Workload wl = createWorkload(0, 64, 1000, "10s", 1, 0.5, "uniform");
                            wl.validate();
                        })
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("keyspaceSize");
    }

    @Test
    void invalidReadRatio() {
        assertThatThrownBy(
                        () -> {
                            Workload wl = createWorkload(100, 64, 1000, "10s", 1, 1.5, "uniform");
                            wl.validate();
                        })
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("readRatio");
    }

    @Test
    void invalidDistribution() {
        assertThatThrownBy(
                        () -> {
                            Workload wl = createWorkload(100, 64, 1000, "10s", 1, 0.5, "invalid");
                            wl.validate();
                        })
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("keyDistribution");
    }

    @Test
    void defaultDistribution() {
        Workload wl = createWorkload(100, 64, 1000, "10s", 1, 0.5, null);
        wl.applyDefaults();
        assertThat(wl.keyDistribution()).isEqualTo("uniform");
    }

    @Test
    void parseDuration() {
        assertThat(Workload.parseDuration("10s").getSeconds()).isEqualTo(10);
        assertThat(Workload.parseDuration("5m").toMinutes()).isEqualTo(5);
        assertThat(Workload.parseDuration("100ms").toMillis()).isEqualTo(100);
    }

    private static Workload createWorkload(
            long keyspaceSize,
            int valueSize,
            double targetRate,
            String duration,
            int parallelism,
            double readRatio,
            String keyDistribution) {
        // Use Jackson to create from a map for simplicity
        com.fasterxml.jackson.databind.ObjectMapper mapper =
                new com.fasterxml.jackson.databind.ObjectMapper();
        java.util.Map<String, Object> map = new java.util.LinkedHashMap<>();
        map.put("keyspaceSize", keyspaceSize);
        map.put("valueSize", valueSize);
        map.put("targetRate", targetRate);
        map.put("duration", duration);
        map.put("parallelism", parallelism);
        map.put("readRatio", readRatio);
        if (keyDistribution != null) {
            map.put("keyDistribution", keyDistribution);
        }
        return mapper.convertValue(map, Workload.class);
    }
}
