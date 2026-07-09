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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

/** Loads a list of {@link SessionExperiment}s from YAML, mirroring {@code Workloads}. */
public class SessionExperiments {

    private static final ObjectMapper YAML = new ObjectMapper(new YAMLFactory());

    @JsonProperty private boolean exitWhenFinish = true;
    @JsonProperty private List<SessionExperiment> items = List.of();

    public boolean exitWhenFinish() {
        return exitWhenFinish;
    }

    public List<SessionExperiment> items() {
        return items;
    }

    public static SessionExperiments load(Path path) throws IOException {
        SessionExperiments e = YAML.readValue(path.toFile(), SessionExperiments.class);
        for (int i = 0; i < e.items.size(); i++) {
            SessionExperiment exp = e.items.get(i);
            exp.applyDefaults();
            try {
                exp.validate();
            } catch (IllegalArgumentException ex) {
                throw new IllegalArgumentException("experiment " + i + ": " + ex.getMessage(), ex);
            }
        }
        return e;
    }
}
