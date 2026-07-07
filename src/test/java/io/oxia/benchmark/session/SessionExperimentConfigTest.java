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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class SessionExperimentConfigTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @ParameterizedTest
    @ValueSource(
            strings = {
                "conf/session-s1-capacity.yaml",
                "conf/session-s2-churn.yaml",
                "conf/session-s3-cleanup.yaml",
                "conf/session-s4-storm.yaml",
                "conf/session-test.yaml"
            })
    void shippedConfigsParseAndValidate(String file) throws Exception {
        SessionExperiments experiments = SessionExperiments.load(Path.of(file));
        assertThat(experiments.items()).isNotEmpty();
        // load() has already applied defaults and validated; assert the fairness knobs are set.
        for (SessionExperiment e : experiments.items()) {
            assertThat(e.type()).isIn("S1", "S2", "S3", "S4");
            assertThat(e.sessionTimeout()).isGreaterThan(Duration.ZERO);
            assertThat(e.ephemeralsPerSession()).isGreaterThanOrEqualTo(1);
        }
    }

    @Test
    void appliesDefaults() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("type", "S3");
        map.put("backgroundSessions", 10);
        SessionExperiment e = MAPPER.convertValue(map, SessionExperiment.class);
        e.applyDefaults();
        e.validate();

        assertThat(e.sessionTimeout()).isEqualTo(Duration.ofSeconds(10));
        assertThat(e.ephemeralsPerSession()).isEqualTo(1);
        assertThat(e.keyPrefix()).isEqualTo("bench/sess");
        assertThat(e.trials()).isEqualTo(100);
        assertThat(e.foregroundReadRatio()).isEqualTo(0.5);
        assertThat(e.departure()).isEqualTo("graceful");
    }

    @Test
    void rejectsUnknownType() {
        SessionExperiment e = experiment(Map.of("type", "S9"));
        assertThatThrownBy(e::validate).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsS1WithoutSweep() {
        SessionExperiment e = experiment(Map.of("type", "S1", "foregroundRate", 1000));
        assertThatThrownBy(e::validate)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("sessionsSweep");
    }

    @Test
    void rejectsS4KillFractionOutOfRange() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("type", "S4");
        map.put("sessions", 100);
        map.put("killFractionSweep", List.of(0.5, 1.5));
        SessionExperiment e = experiment(map);
        assertThatThrownBy(e::validate)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("(0, 1]");
    }

    private static SessionExperiment experiment(Map<String, Object> map) {
        SessionExperiment e = MAPPER.convertValue(map, SessionExperiment.class);
        e.applyDefaults();
        return e;
    }
}
