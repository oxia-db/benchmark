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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

public class Workloads {

  private static final ObjectMapper YAML = new ObjectMapper(new YAMLFactory());

  @JsonProperty private Metadata metadata = new Metadata();
  @JsonProperty private boolean exitWhenFinish;
  @JsonProperty private List<Workload> items = List.of();

  public Metadata metadata() {
    return metadata;
  }

  public boolean exitWhenFinish() {
    return exitWhenFinish;
  }

  public List<Workload> items() {
    return items;
  }

  public static Workloads load(Path path) throws IOException {
    Workloads w = YAML.readValue(path.toFile(), Workloads.class);
    for (int i = 0; i < w.items.size(); i++) {
      Workload wl = w.items.get(i);
      wl.applyDefaults();
      try {
        wl.validate();
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException("workload " + i + ": " + e.getMessage(), e);
      }
    }
    return w;
  }

  public static class Metadata {
    @JsonProperty private int serverNum;

    public int serverNum() {
      return serverNum;
    }
  }
}
