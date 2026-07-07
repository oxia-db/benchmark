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
    void s3RecordsPerTrialCleanupVisibility() throws Exception {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("type", "S3");
        map.put("sessionTimeout", "300ms");
        map.put("ephemeralsPerSession", 1);
        map.put("createConcurrency", 8);
        map.put("warmup", "0s");
        map.put("backgroundSessions", 5);
        map.put("trials", 4);
        map.put("settleTimeout", "2s");
        map.put("foregroundRate", 0); // idle-only phase

        SessionResult r = run(map);

        assertThat(r.type).isEqualTo("S3");
        assertThat(r.cleanupTrials).hasSize(4); // idle phase only
        for (SessionResult.CleanupTrial t : r.cleanupTrials) {
            assertThat(t.load).isEqualTo("idle");
            assertThat(t.notified).isTrue();
            assertThat(t.gone).isTrue();
            // EXCESS is measured beyond (t_hb + timeout); with the mock expiring exactly at the deadline
            // it should be a small non-negative latency, never a large or wildly negative value.
            assertThat(t.excessMs).isBetween(-50.0, 1500.0);
        }
    }

    @Test
    void s4ProducesCleanupCurvePerKillFraction() throws Exception {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("type", "S4");
        map.put("sessionTimeout", "300ms");
        map.put("ephemeralsPerSession", 1);
        map.put("createConcurrency", 8);
        map.put("warmup", "0s");
        map.put("sessions", 20);
        map.put("killFractionSweep", List.of(0.5, 1.0));
        map.put("sampleKeys", 10);
        map.put("sampleIntervalMs", 50);
        map.put("settleTimeout", "2s");
        map.put("foregroundRate", 0);

        SessionResult r = run(map);

        assertThat(r.storm).hasSize(2);
        SessionResult.StormRun half = r.storm.get(0);
        assertThat(half.killFraction).isEqualTo(0.5);
        assertThat(half.killed).isEqualTo(10);
        assertThat(half.timeline).isNotEmpty();
        assertThat(half.completionMs).isNotNaN().isPositive();

        SessionResult.StormRun full = r.storm.get(1);
        assertThat(full.killFraction).isEqualTo(1.0);
        assertThat(full.killed).isEqualTo(20);
        assertThat(full.completionMs).isNotNaN().isPositive();
        // The final sample of a completed run confirms (near) full deletion.
        assertThat(full.timeline.get(full.timeline.size() - 1).fractionDeleted).isGreaterThan(0.9);
    }

    private SessionResult run(Map<String, Object> map) throws Exception {
        SessionExperiment exp = MAPPER.convertValue(map, SessionExperiment.class);
        exp.applyDefaults();
        exp.validate();
        return new SessionExperimentRunner(exp, driver, "test").run();
    }
}
