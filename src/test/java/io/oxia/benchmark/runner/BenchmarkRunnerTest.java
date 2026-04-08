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

package io.oxia.benchmark.runner;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.oxia.benchmark.driver.KVStoreDriver;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.LongAdder;
import org.junit.jupiter.api.Test;

class BenchmarkRunnerTest {

  @Test
  void writeOnlyWorkload() throws Exception {
    MockDriver driver = new MockDriver();
    Workload wl = createWorkload(0.0, "order");

    new BenchmarkRunner(wl, driver).run();

    assertThat(driver.putCount.sum()).isGreaterThan(0);
    assertThat(driver.getCount.sum()).isEqualTo(0);
  }

  @Test
  void readOnlyWorkload() throws Exception {
    MockDriver driver = new MockDriver();
    Workload wl = createWorkload(1.0, "uniform");

    new BenchmarkRunner(wl, driver).run();

    assertThat(driver.getCount.sum()).isGreaterThan(0);
    assertThat(driver.putCount.sum()).isEqualTo(0);
  }

  @Test
  void mixedWorkload() throws Exception {
    MockDriver driver = new MockDriver();
    Workload wl = createWorkload(0.5, "zipf");

    new BenchmarkRunner(wl, driver).run();

    assertThat(driver.putCount.sum()).isGreaterThan(0);
    assertThat(driver.getCount.sum()).isGreaterThan(0);
  }

  private static Workload createWorkload(double readRatio, String distribution) {
    ObjectMapper mapper = new ObjectMapper();
    Map<String, Object> map = new LinkedHashMap<>();
    map.put("readRatio", readRatio);
    map.put("keyspaceSize", 1000);
    map.put("keyDistribution", distribution);
    map.put("valueSize", 64);
    map.put("targetRate", 10000.0);
    map.put("duration", "2s");
    map.put("parallelism", 2);
    return mapper.convertValue(map, Workload.class);
  }

  static class MockDriver implements KVStoreDriver {
    final LongAdder putCount = new LongAdder();
    final LongAdder getCount = new LongAdder();

    @Override
    public String name() {
      return "mock";
    }

    @Override
    public void init(Map<String, Object> config) {}

    @Override
    public CompletableFuture<Void> put(String key, byte[] value) {
      putCount.increment();
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> get(String key) {
      getCount.increment();
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public void close() {}
  }
}
