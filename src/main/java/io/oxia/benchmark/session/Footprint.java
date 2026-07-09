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

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

/**
 * A snapshot of this worker's client-side cost: open sockets, live JVM threads, and used heap (MB)
 * — the resources a session-heavy client actually consumes, reported per 10k sessions so
 * per-session overhead is measured rather than hidden. Sockets are counted from {@code
 * /proc/self/fd} on Linux (the deployment target); on platforms without /proc (local dev on macOS)
 * they are reported as -1 so callers render "n/a" rather than a misleading zero. Heap is read after
 * a {@link System#gc()} hint so it reflects live, retained memory rather than allocation churn.
 */
public record Footprint(long sockets, int threads, double heapMB) {

    private static final Path FD_DIR = Path.of("/proc/self/fd");

    public static Footprint sample() {
        Runtime rt = Runtime.getRuntime();
        System.gc();
        double heapMB = (rt.totalMemory() - rt.freeMemory()) / (1024.0 * 1024.0);
        int threads = ManagementFactory.getThreadMXBean().getThreadCount();
        return new Footprint(countSockets(), threads, heapMB);
    }

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

    private static long countSockets() {
        if (!Files.isDirectory(FD_DIR)) {
            return -1;
        }
        try (Stream<Path> fds = Files.list(FD_DIR)) {
            return fds.filter(Footprint::isSocket).count();
        } catch (IOException e) {
            return -1;
        }
    }

    private static boolean isSocket(Path fd) {
        try {
            return Files.readSymbolicLink(fd).toString().startsWith("socket:");
        } catch (IOException e) {
            // fd closed between listing and readlink — just skip it.
            return false;
        }
    }
}
