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
package io.oxia.benchmark.driver;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

/**
 * @param driver which driver implementation to build
 * @param label optional display name used in results and metrics instead of the driver's own (e.g.
 *     "oxia-3" vs "oxia-12" to distinguish cluster sizes of the same backend)
 * @param config driver-specific settings
 */
public record DriverConfig(String driver, String label, Map<String, Object> config) {

    private static final ObjectMapper YAML = new ObjectMapper(new YAMLFactory());

    @SuppressWarnings("unchecked")
    public static DriverConfig load(Path path) throws IOException {
        var map = YAML.readValue(path.toFile(), Map.class);
        String driver = (String) map.get("driver");
        String label = (String) map.get("label");
        Map<String, Object> config = (Map<String, Object>) map.get("config");
        return new DriverConfig(driver, label, config != null ? config : Map.of());
    }
}
