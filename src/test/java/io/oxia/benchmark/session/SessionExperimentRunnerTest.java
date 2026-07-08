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

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class SessionExperimentRunnerTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final MockSessionDriver driver = new MockSessionDriver();

    @AfterEach
    void tearDown() {
        driver.close();
    }

    @Test
    void s1SweepsSessionsAndMeasuresForegroundPerPoint() throws Exception {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("type", "S1");
        map.put("sessionTimeout", "1s");
        map.put("ephemeralsPerSession", 2);
        map.put("createConcurrency", 16);
        map.put("warmup", "0s");
        map.put("holdDuration", "300ms");
        map.put("foregroundRate", 500);
        map.put("foregroundKeyspaceSize", 1000);
        map.put("foregroundParallelism", 2);
        map.put("sessionsSweep", List.of(10, 25));

        SessionResult r = run(map);

        assertThat(r.type).isEqualTo("S1");
        // Baseline (N=0) plus one point per sweep entry.
        assertThat(r.capacity).hasSize(3);
        assertThat(r.capacity.get(0).sessions).isZero();
        assertThat(r.capacity.get(1).sessions).isEqualTo(10);
        assertThat(r.capacity.get(2).sessions).isEqualTo(25);
        for (SessionResult.CapacityPoint p : r.capacity) {
            assertThat(p.foregroundThroughput).isGreaterThan(0);
        }
    }

    @Test
    void s2SustainsChurnAndRecordsEstablishLatency() throws Exception {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("type", "S2");
        map.put("sessionTimeout", "1s");
        map.put("ephemeralsPerSession", 1);
        map.put("createConcurrency", 32);
        map.put("warmup", "0s");
        map.put("holdDuration", "500ms");
        map.put("departure", "graceful");
        map.put("foregroundRate", 0);
        map.put("churnRateSweep", List.of(100.0));

        SessionResult r = run(map);

        assertThat(r.type).isEqualTo("S2");
        assertThat(r.departure).isEqualTo("graceful");
        assertThat(r.churn).hasSize(1);
        SessionResult.ChurnPoint p = r.churn.get(0);
        assertThat(p.targetRate).isEqualTo(100.0);
        assertThat(p.established).isGreaterThan(0);
        assertThat(p.achievedCreateRate).isGreaterThan(0);
        // Mock ops complete inline, so a 100/s target is trivially sustained.
        assertThat(p.sustained).isTrue();
        assertThat(p.establishFailed).isZero();
    }

    @Test
    void s2AbandonLeavesKeysToExpireServerSide() throws Exception {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("type", "S2");
        map.put("sessionTimeout", "200ms");
        map.put("ephemeralsPerSession", 1);
        map.put("createConcurrency", 32);
        map.put("warmup", "0s");
        map.put("holdDuration", "300ms");
        map.put("departure", "abandon");
        map.put("foregroundRate", 0);
        map.put("churnRateSweep", List.of(50.0));

        SessionResult r = run(map);

        assertThat(r.churn.get(0).achievedDepartRate).isGreaterThan(0);
        // Abandoned sessions' keys expire via the mock's timeout path; give it time to drain.
        Thread.sleep(500);
        assertThat(driver.presentCount()).isZero();
    }

    private SessionResult run(Map<String, Object> map) throws Exception {
        SessionExperiment exp = MAPPER.convertValue(map, SessionExperiment.class);
        exp.applyDefaults();
        exp.validate();
        return new SessionExperimentRunner(exp, driver, "test").run();
    }
}
