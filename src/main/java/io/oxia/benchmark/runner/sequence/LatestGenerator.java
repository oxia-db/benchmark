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
package io.oxia.benchmark.runner.sequence;

/**
 * "Latest" distribution for YCSB workload D (read-latest): the highest key indices are the most
 * popular. When the load phase writes sequentially (the {@code order} distribution), those are the
 * most recently inserted keys, so this skews reads toward the newest records. Implemented as a Zipf
 * reflected to the top of the range, mirroring YCSB's {@code max - zipfian(max)}.
 */
final class LatestGenerator implements SequenceGenerator {

    private final long imax;
    private final ZipfGenerator zipf;

    LatestGenerator(long maxSequence) {
        this.imax = maxSequence - 1;
        this.zipf = new ZipfGenerator(maxSequence);
    }

    @Override
    public long next() {
        return imax - zipf.next();
    }
}
