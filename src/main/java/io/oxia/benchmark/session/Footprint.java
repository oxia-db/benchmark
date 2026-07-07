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

/**
 * A snapshot of this worker's client-side cost: open sockets, live JVM threads, and used heap (MB).
 * These are the resources a session-heavy client actually consumes — the number the experiments
 * report per 10k sessions so per-session overhead is measured rather than hidden. {@code sockets}
 * is -1 when the platform can't enumerate file descriptors (see {@code FootprintSampler}).
 */
public record Footprint(long sockets, int threads, double heapMB) {

    /** Component-wise delta (this − baseline), for isolating the cost added by the sessions. */
    public Footprint minus(Footprint baseline) {
        long s = sockets < 0 || baseline.sockets < 0 ? -1 : sockets - baseline.sockets;
        return new Footprint(s, threads - baseline.threads, heapMB - baseline.heapMB);
    }

    /** Scale this delta to a per-10k-sessions rate; sockets stay -1 if unavailable. */
    public Footprint per10k(long sessions) {
        if (sessions <= 0) {
            return this;
        }
        double factor = 10_000.0 / sessions;
        long s = sockets < 0 ? -1 : Math.round(sockets * factor);
        return new Footprint(s, (int) Math.round(threads * factor), heapMB * factor);
    }
}
