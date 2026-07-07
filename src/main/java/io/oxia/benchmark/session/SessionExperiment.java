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
 * One session/ephemeral experiment (S1 capacity, S2 churn, S3 cleanup-visibility, S4 expiry storm).
 * The common fields fix the fairness knobs shared by every backend — identical session timeout,
 * ephemerals-per-session ({@code k}), and key namespace; the per-type fields drive the sweep.
 * Several experiments can be listed in one file and run sequentially, exactly like {@link
 * Workload}s.
 */
public class SessionExperiment {

    private static final Set<String> VALID_TYPES = Set.of("S1", "S2", "S3", "S4");
    private static final Set<String> VALID_DEPARTURES = Set.of("graceful", "abandon");

    @JsonProperty private String name;
    @JsonProperty private String description;
    @JsonProperty private String type; // S1 | S2 | S3 | S4

    // ---- shared session parameters (identical across systems for fairness) ----------------------
    @JsonProperty private String sessionTimeout; // default 10s
    @JsonProperty private int ephemeralsPerSession; // k; run k=1 and k=10 as separate items
    @JsonProperty private int ephemeralValueSize; // bytes
    @JsonProperty private String keyPrefix; // ephemeral key namespace root
    @JsonProperty private int createConcurrency; // max in-flight session establishes
    @JsonProperty private String warmup;

    // ---- foreground load (S1 always; S3 load phase; S4 timeline) --------------------------------
    @JsonProperty private double foregroundRate; // ops/s per worker at the measured operating point
    @JsonProperty private double foregroundReadRatio; // YCSB-A-style default 0.5
    @JsonProperty private long foregroundKeyspaceSize;
    @JsonProperty private String foregroundKeyDistribution;
    @JsonProperty private int foregroundValueSize;
    @JsonProperty private int foregroundParallelism;

    // ---- S1 capacity ----------------------------------------------------------------------------
    @JsonProperty private List<Long> sessionsSweep; // live-session counts N
    @JsonProperty private String holdDuration; // measure window per sweep point (S1/S2)

    // ---- S2 churn -------------------------------------------------------------------------------
    @JsonProperty private List<Double> churnRateSweep; // sessions/s r
    @JsonProperty private String departure; // graceful | abandon

    // ---- S3 cleanup-visibility ------------------------------------------------------------------
    @JsonProperty private long backgroundSessions; // N live background sessions
    @JsonProperty private int trials; // per load condition (idle, then ~80% load)
    @JsonProperty private String settleTimeout; // max wait per trial for gone/notify

    // ---- S4 expiry storm ------------------------------------------------------------------------
    @JsonProperty private long sessions; // N live sessions before the storm
    @JsonProperty private List<Double> killFractionSweep; // e.g. 0.1, 0.5, 1.0
    @JsonProperty private int sampleKeys; // killed keys sampled for the completion curve
    @JsonProperty private int sampleIntervalMs; // completion-curve sampling resolution

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

    public long backgroundSessions() {
        return backgroundSessions;
    }

    public int trials() {
        return trials;
    }

    public Duration settleTimeout() {
        return Workload.parseDuration(settleTimeout);
    }

    public long sessions() {
        return sessions;
    }

    public List<Double> killFractionSweep() {
        return killFractionSweep == null ? List.of() : killFractionSweep;
    }

    public int sampleKeys() {
        return sampleKeys;
    }

    public int sampleIntervalMs() {
        return sampleIntervalMs;
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
        if (trials <= 0) {
            trials = 100;
        }
        if (settleTimeout == null || settleTimeout.isEmpty()) {
            settleTimeout = "30s";
        }
        if (sampleKeys <= 0) {
            sampleKeys = 2000;
        }
        if (sampleIntervalMs <= 0) {
            sampleIntervalMs = 200;
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
            case "S1" -> {
                if (sessionsSweep().isEmpty()) {
                    throw new IllegalArgumentException("S1 requires a non-empty sessionsSweep");
                }
                if (foregroundRate <= 0) {
                    throw new IllegalArgumentException(
                            "S1 requires foregroundRate > 0 (the operating point)");
                }
            }
            case "S2" -> {
                if (churnRateSweep().isEmpty()) {
                    throw new IllegalArgumentException("S2 requires a non-empty churnRateSweep");
                }
            }
            case "S3" -> {
                if (backgroundSessions <= 0) {
                    throw new IllegalArgumentException("S3 requires backgroundSessions > 0");
                }
                if (trials <= 0) {
                    throw new IllegalArgumentException("S3 requires trials > 0");
                }
            }
            case "S4" -> {
                if (sessions <= 0) {
                    throw new IllegalArgumentException("S4 requires sessions > 0");
                }
                if (killFractionSweep().isEmpty()) {
                    throw new IllegalArgumentException("S4 requires a non-empty killFractionSweep");
                }
                for (double f : killFractionSweep()) {
                    if (f <= 0 || f > 1.0) {
                        throw new IllegalArgumentException("S4 killFraction values must be in (0, 1]");
                    }
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
