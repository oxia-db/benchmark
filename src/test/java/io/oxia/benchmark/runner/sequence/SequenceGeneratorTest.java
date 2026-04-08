/*
 * Copyright 2025 The Oxia Authors
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import org.junit.jupiter.api.Test;

class SequenceGeneratorTest {

  @Test
  void orderGeneratorProducesSequentialValues() {
    SequenceGenerator gen = SequenceGenerator.create("order", 5);
    assertThat(gen.next()).isEqualTo(1);
    assertThat(gen.next()).isEqualTo(2);
    assertThat(gen.next()).isEqualTo(3);
    assertThat(gen.next()).isEqualTo(4);
    assertThat(gen.next()).isEqualTo(0); // wraps around
    assertThat(gen.next()).isEqualTo(1);
  }

  @Test
  void uniformGeneratorProducesValuesInRange() {
    SequenceGenerator gen = SequenceGenerator.create("uniform", 100);
    for (int i = 0; i < 1000; i++) {
      long val = gen.next();
      assertThat(val).isBetween(0L, 99L);
    }
  }

  @Test
  void zipfGeneratorProducesValuesInRange() {
    SequenceGenerator gen = SequenceGenerator.create("zipf", 100);
    for (int i = 0; i < 1000; i++) {
      long val = gen.next();
      assertThat(val).isBetween(0L, 99L);
    }
  }

  @Test
  void zipfGeneratorFavorsLowValues() {
    SequenceGenerator gen = SequenceGenerator.create("zipf", 1000);
    int lowCount = 0;
    int total = 10000;
    for (int i = 0; i < total; i++) {
      if (gen.next() < 100) {
        lowCount++;
      }
    }
    // With zipf distribution (s=1.1), majority should be in the low range
    assertThat(lowCount).isGreaterThan(total / 2);
  }

  @Test
  void orderGeneratorIsConcurrencySafe() throws InterruptedException {
    SequenceGenerator gen = SequenceGenerator.create("order", 10000);
    int threadCount = 4;
    int opsPerThread = 2500;
    Set<Long> allValues = ConcurrentHashMap.newKeySet();
    CountDownLatch latch = new CountDownLatch(threadCount);

    for (int t = 0; t < threadCount; t++) {
      new Thread(
              () -> {
                for (int i = 0; i < opsPerThread; i++) {
                  allValues.add(gen.next());
                }
                latch.countDown();
              })
          .start();
    }
    latch.await();
    // All values should be unique (no collisions with CAS)
    assertThat(allValues).hasSize(threadCount * opsPerThread);
  }

  @Test
  void uniformGeneratorProducesDistinctValues() {
    SequenceGenerator gen = SequenceGenerator.create("uniform", 1_000_000);
    Set<Long> values = new HashSet<>();
    for (int i = 0; i < 100; i++) {
      values.add(gen.next());
    }
    // With 1M keyspace, 100 samples should all be different (very high probability)
    assertThat(values.size()).isGreaterThan(90);
  }

  @Test
  void unknownDistributionThrows() {
    assertThatThrownBy(() -> SequenceGenerator.create("invalid", 100))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("invalid");
  }
}
