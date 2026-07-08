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

import com.fasterxml.jackson.annotation.JsonProperty;
import io.oxia.benchmark.runner.Workload;
import java.time.Duration;
import java.util.List;
import java.util.Set;

/**
 * One session/ephemeral experiment ({@code capacity} or {@code churn}). The common fields fix the
 * fairness knobs shared by every backend — identical session timeout, ephemerals-per-session
 * ({@code k}), and key namespace; the per-type fields drive the sweep. Several experiments can be
 * listed in one file and run sequentially, exactly like {@link Workload}s.
 */
public class SessionExperiment {

    private static final Set<String> VALID_TYPES = Set.of("capacity", "churn");
    private static final Set<String> VALID_DEPARTURES = Set.of("graceful", "abandon");

    @JsonProperty private String name;
    @JsonProperty private String description;
    @JsonProperty private String type; // capacity | churn

    // ---- shared session parameters (identical across systems for fairness) ----------------------
    @JsonProperty private String sessionTimeout; // default 10s
    @JsonProperty private int ephemeralsPerSession; // k; run k=1 and k=10 as separate items
    @JsonProperty private int ephemeralValueSize; // bytes
    @JsonProperty private String keyPrefix; // ephemeral key namespace root
    @JsonProperty private int createConcurrency; // max in-flight session establishes
    @JsonProperty private String warmup;

    // ---- foreground load (capacity) --------------------------------------------------------------
    @JsonProperty private double foregroundRate; // ops/s per worker at the measured operating point
    @JsonProperty private double foregroundReadRatio; // YCSB-A-style default 0.5
    @JsonProperty private long foregroundKeyspaceSize;
    @JsonProperty private String foregroundKeyDistribution;
    @JsonProperty private int foregroundValueSize;
    @JsonProperty private int foregroundParallelism;

    // ---- capacity -------------------------------------------------------------------------------
    @JsonProperty private List<Long> sessionsSweep; // live-session counts N
    @JsonProperty private String holdDuration; // measure window per sweep point (both types)

    // ---- churn ----------------------------------------------------------------------------------
    @JsonProperty private List<Double> churnRateSweep; // sessions/s r
    @JsonProperty private String departure; // graceful | abandon

    public String name() {
        return name;
    }

    public String description() {
        return description;
    }

    public String type() {
        return type;
    }

    public Duration sessionTimeout() {
        return Workload.parseDuration(sessionTimeout);
    }

    public int ephemeralsPerSession() {
        return ephemeralsPerSession;
    }

    public int ephemeralValueSize() {
        return ephemeralValueSize;
    }

    public String keyPrefix() {
        return keyPrefix;
    }

    public int createConcurrency() {
        return createConcurrency;
    }

    public Duration warmup() {
        return warmup == null || warmup.isEmpty() ? Duration.ZERO : Workload.parseDuration(warmup);
    }

    public double foregroundRate() {
        return foregroundRate;
    }

    public double foregroundReadRatio() {
        return foregroundReadRatio;
    }

    public long foregroundKeyspaceSize() {
        return foregroundKeyspaceSize;
    }

    public String foregroundKeyDistribution() {
        return foregroundKeyDistribution;
    }

    public int foregroundValueSize() {
        return foregroundValueSize;
    }

    public int foregroundParallelism() {
        return foregroundParallelism;
    }

    public List<Long> sessionsSweep() {
        return sessionsSweep == null ? List.of() : sessionsSweep;
    }

    public Duration holdDuration() {
        return Workload.parseDuration(holdDuration);
    }

    public List<Double> churnRateSweep() {
        return churnRateSweep == null ? List.of() : churnRateSweep;
    }

    public String departure() {
        return departure;
    }

    public void applyDefaults() {
        if (sessionTimeout == null || sessionTimeout.isEmpty()) {
            sessionTimeout = "10s";
        }
        if (ephemeralsPerSession <= 0) {
            ephemeralsPerSession = 1;
        }
        if (ephemeralValueSize <= 0) {
            ephemeralValueSize = 16;
        }
        if (keyPrefix == null || keyPrefix.isEmpty()) {
            keyPrefix = "bench/sess";
        }
        if (createConcurrency <= 0) {
            createConcurrency = 256;
        }
        if (foregroundReadRatio <= 0) {
            foregroundReadRatio = 0.5; // YCSB-A mix
        }
        if (foregroundKeyspaceSize <= 0) {
            foregroundKeyspaceSize = 1_000_000;
        }
        if (foregroundKeyDistribution == null || foregroundKeyDistribution.isEmpty()) {
            foregroundKeyDistribution = "zipf";
        }
        if (foregroundValueSize <= 0) {
            foregroundValueSize = 64;
        }
        if (foregroundParallelism <= 0) {
            foregroundParallelism = 8;
        }
        if (holdDuration == null || holdDuration.isEmpty()) {
            holdDuration = "60s";
        }
        if (departure == null || departure.isEmpty()) {
            departure = "graceful";
        }
    }

    public void validate() {
        if (type == null || !VALID_TYPES.contains(type)) {
            throw new IllegalArgumentException("type must be one of " + VALID_TYPES + ", got: " + type);
        }
        if (sessionTimeout().isZero() || sessionTimeout().isNegative()) {
            throw new IllegalArgumentException("sessionTimeout must be greater than 0");
        }
        if (!VALID_DEPARTURES.contains(departure)) {
            throw new IllegalArgumentException("departure must be one of " + VALID_DEPARTURES);
        }
        switch (type) {
            case "capacity" -> {
                if (sessionsSweep().isEmpty()) {
                    throw new IllegalArgumentException("capacity requires a non-empty sessionsSweep");
                }
                if (foregroundRate <= 0) {
                    throw new IllegalArgumentException(
                            "capacity requires foregroundRate > 0 (the operating point)");
                }
            }
            case "churn" -> {
                if (churnRateSweep().isEmpty()) {
                    throw new IllegalArgumentException("churn requires a non-empty churnRateSweep");
                }
            }
            default -> throw new IllegalArgumentException("unsupported type: " + type);
        }
    }

    @Override
    public String toString() {
        return String.format(
                "{name=%s, type=%s, sessionTimeout=%s, k=%d, keyPrefix=%s, foregroundRate=%.0f}",
                name, type, sessionTimeout, ephemeralsPerSession, keyPrefix, foregroundRate);
    }
}
