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

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Duration;
import java.util.Set;

public class Workload {

    private static final Set<String> VALID_DISTRIBUTIONS = Set.of("uniform", "zipf", "order");

    @JsonProperty private String name; // optional human label, surfaced in results and the report
    @JsonProperty private String description; // optional longer note, surfaced in the report
    @JsonProperty private double readRatio;
    @JsonProperty private long keyspaceSize;
    @JsonProperty private String keyDistribution;
    @JsonProperty private int valueSize;
    @JsonProperty private double targetRate;
    @JsonProperty private String duration;
    @JsonProperty private String warmup;
    @JsonProperty private int parallelism;

    public String name() {
        return name;
    }

    public String description() {
        return description;
    }

    public double readRatio() {
        return readRatio;
    }

    public long keyspaceSize() {
        return keyspaceSize;
    }

    public String keyDistribution() {
        return keyDistribution;
    }

    public int valueSize() {
        return valueSize;
    }

    public double targetRate() {
        return targetRate;
    }

    public Duration duration() {
        return parseDuration(duration);
    }

    public Duration warmup() {
        return warmup == null || warmup.isEmpty() ? Duration.ofSeconds(10) : parseDuration(warmup);
    }

    public int parallelism() {
        return parallelism;
    }

    public void applyDefaults() {
        if (keyDistribution == null || keyDistribution.isEmpty()) {
            keyDistribution = "uniform";
        }
    }

    public void validate() {
        if (keyspaceSize <= 0) {
            throw new IllegalArgumentException("keyspaceSize must be greater than 0");
        }
        if (valueSize <= 0) {
            throw new IllegalArgumentException("valueSize must be greater than 0");
        }
        if (targetRate <= 0) {
            throw new IllegalArgumentException("targetRate must be greater than 0");
        }
        if (duration == null || duration.isEmpty()) {
            throw new IllegalArgumentException("duration must be specified");
        }
        if (duration().isZero() || duration().isNegative()) {
            throw new IllegalArgumentException("duration must be greater than 0");
        }
        if (parallelism <= 0) {
            throw new IllegalArgumentException("parallelism must be greater than 0");
        }
        if (readRatio < 0 || readRatio > 1) {
            throw new IllegalArgumentException("readRatio must be between 0 and 1");
        }
        if (!VALID_DISTRIBUTIONS.contains(keyDistribution)) {
            throw new IllegalArgumentException("unsupported keyDistribution: " + keyDistribution);
        }
    }

    @Override
    public String toString() {
        return String.format(
                "{readRatio=%.1f, keyspaceSize=%d, keyDistribution=%s, valueSize=%d,"
                        + " targetRate=%.0f, duration=%s, warmup=%s, parallelism=%d}",
                readRatio,
                keyspaceSize,
                keyDistribution,
                valueSize,
                targetRate,
                duration,
                warmup(),
                parallelism);
    }

    static Duration parseDuration(String s) {
        if (s == null || s.isEmpty()) {
            return Duration.ZERO;
        }
        if (s.endsWith("ms")) {
            return Duration.ofMillis(Long.parseLong(s.substring(0, s.length() - 2)));
        }
        if (s.endsWith("s") && !s.endsWith("ms")) {
            return Duration.ofSeconds(Long.parseLong(s.substring(0, s.length() - 1)));
        }
        if (s.endsWith("m") && !s.endsWith("ms")) {
            return Duration.ofMinutes(Long.parseLong(s.substring(0, s.length() - 1)));
        }
        if (s.endsWith("h")) {
            return Duration.ofHours(Long.parseLong(s.substring(0, s.length() - 1)));
        }
        return Duration.parse("PT" + s);
    }
}
