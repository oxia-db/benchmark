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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.List;

/**
 * One worker's result for one session experiment, serialized as a single JSON line (mirroring
 * {@code WorkloadResult}). Only the block matching {@link #type} is populated; the rest stay null
 * and are omitted. {@code SessionReportCommand} reads these back and flattens the populated block
 * into the per-experiment CSV. Unknown fields are ignored so a newer worker's result still parses
 * with an older report tool.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class SessionResult {

    public int index; // experiment position in the run
    public String instanceId; // worker / pod id
    public String name;
    public String description;
    public String type; // S1 | S2
    public String driver;

    // Fairness knobs, echoed for the report and cross-checking that they matched across systems.
    public String sessionTimeout;
    public double sessionTimeoutMs;
    public int ephemeralsPerSession;

    public List<CapacityPoint> capacity; // S1
    public List<ChurnPoint> churn; // S2
    public String departure; // S2

    /** S1: one operating point in the live-session sweep. */
    public static class CapacityPoint {
        public long sessions; // N live sessions
        public double foregroundThroughput; // ops/s over the hold window
        public double foregroundP50;
        public double foregroundP99;
        public double foregroundMax;
        public long foregroundFailed;
        public double degradationPct; // vs the N=0 baseline throughput
        // Client-side cost added by the sessions (this point minus the N=0 baseline).
        public long footprintSocketsPer10k = -1;
        public int footprintThreadsPer10k;
        public double footprintHeapMBPer10k;
    }

    /** S2: one churn rate in the sweep. */
    public static class ChurnPoint {
        public double targetRate; // r sessions/s attempted
        public double achievedCreateRate; // established/s actually sustained
        public double achievedDepartRate; // departed/s
        public long established;
        public long establishFailed;
        public long fellBehind; // ticks the create loop could not keep up (knee indicator)
        public double establishP50;
        public double establishP99;
        public double establishMax;
        public boolean sustained; // achieved >= 95% of target
    }
}
