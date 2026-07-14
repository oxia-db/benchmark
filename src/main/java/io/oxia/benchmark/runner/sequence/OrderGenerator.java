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

import java.util.concurrent.atomic.AtomicLong;

final class OrderGenerator implements SequenceGenerator {

    private final long start;
    private final long end; // exclusive
    private final AtomicLong sequence;

    // Each worker fills the disjoint slice [start, end) of the keyspace: replicated load
    // generators would otherwise all count from 0 and overwrite the same keys N times over.
    // The last worker's slice absorbs the division remainder.
    OrderGenerator(long maxSequence, int workerIndex, int workerCount) {
        if (workerIndex < 0 || workerIndex >= workerCount) {
            throw new IllegalArgumentException(
                    "workerIndex " + workerIndex + " out of range for workerCount " + workerCount);
        }
        long sliceSize = maxSequence / workerCount;
        this.start = sliceSize * workerIndex;
        this.end = workerIndex == workerCount - 1 ? maxSequence : start + sliceSize;
        this.sequence = new AtomicLong(start);
    }

    @Override
    public long next() {
        while (true) {
            long current = sequence.get();
            long next = current + 1;
            if (next >= end) {
                if (sequence.compareAndSet(current, start)) {
                    return start;
                }
                continue;
            }
            if (sequence.compareAndSet(current, next)) {
                return next;
            }
        }
    }
}
