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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * One worker's measured result for a single workload. The latency distributions are stored as
 * base64-encoded, compressed HdrHistograms so the report step can merge them across workers for
 * exact aggregated percentiles (averaging per-worker percentiles is not valid). Serialized as one
 * JSON line per (worker, workload); public fields keep Jackson mapping trivial. Unknown fields are
 * ignored so a newer worker's result still parses with an older report tool (and vice versa).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class WorkloadResult {

    public int index; // workload position in the run
    public String instanceId; // worker / pod identifier

    public String name; // optional workload label from config
    public String description; // optional workload description from config

    public String driver;
    public double readRatio;
    public long keyspaceSize;
    public String keyDistribution;
    public int valueSize;
    public double targetRate;
    public int parallelism;
    public String duration;

    public double measuredSeconds; // post-warmup measured window
    public long writeCount;
    public long readCount;
    public long failedCount;

    public String writeHistB64; // latency ms, HdrHistogram compressed + base64
    public String readHistB64;
}
