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
import lombok.CustomLog;

/**
 * Samples this worker's client-side footprint (sockets, threads, used heap MB). Sockets are counted
 * from {@code /proc/self/fd} on Linux — the deployment target — by resolving each fd symlink and
 * counting the ones that point at a {@code socket:} inode; on non-Linux platforms (local dev on
 * macOS) socket counting is unavailable and reported as -1, while threads and heap are always
 * available via the JVM. Heap is read after a {@link System#gc()} hint so the number reflects live,
 * retained memory rather than allocation churn.
 */
@CustomLog
public final class FootprintSampler {

    private static final Path FD_DIR = Path.of("/proc/self/fd");

    private FootprintSampler() {}

    public static Footprint sample() {
        return new Footprint(countSockets(), threadCount(), usedHeapMB());
    }

    private static int threadCount() {
        return ManagementFactory.getThreadMXBean().getThreadCount();
    }

    private static double usedHeapMB() {
        Runtime rt = Runtime.getRuntime();
        // Nudge the collector so the reading reflects retained memory, not transient garbage.
        System.gc();
        long used = rt.totalMemory() - rt.freeMemory();
        return used / (1024.0 * 1024.0);
    }

    /**
     * Count open socket file descriptors via /proc (Linux only). Returns -1 where /proc is absent so
     * callers can render the metric as "n/a" rather than a misleading zero.
     */
    private static long countSockets() {
        if (!Files.isDirectory(FD_DIR)) {
            return -1;
        }
        try (Stream<Path> fds = Files.list(FD_DIR)) {
            return fds.filter(FootprintSampler::isSocket).count();
        } catch (IOException e) {
            log.debug().exception(e).log("Could not enumerate /proc/self/fd for socket count");
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
