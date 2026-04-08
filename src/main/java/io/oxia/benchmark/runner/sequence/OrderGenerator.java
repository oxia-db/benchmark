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

    private final long maxSequence;
    private final AtomicLong sequence = new AtomicLong(0);

    OrderGenerator(long maxSequence) {
        this.maxSequence = maxSequence;
    }

    @Override
    public long next() {
        while (true) {
            long current = sequence.get();
            long next = current + 1;
            if (next >= maxSequence) {
                if (sequence.compareAndSet(current, 0)) {
                    return 0;
                }
                continue;
            }
            if (sequence.compareAndSet(current, next)) {
                return next;
            }
        }
    }
}
