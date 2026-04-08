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

public interface SequenceGenerator {

    long next();

    static SequenceGenerator create(String distribution, long maxSequence) {
        return switch (distribution) {
            case "uniform" -> new UniformGenerator(maxSequence);
            case "zipf" -> new ZipfGenerator(maxSequence);
            case "order" -> new OrderGenerator(maxSequence);
            default ->
                    throw new IllegalArgumentException("Unsupported key distribution: " + distribution);
        };
    }
}
