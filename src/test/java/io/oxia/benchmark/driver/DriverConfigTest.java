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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class DriverConfigTest {

    @Test
    void loadOxiaConfig() throws IOException {
        DriverConfig config = DriverConfig.load(Path.of("conf/driver-oxia.yaml"));
        assertThat(config.driver()).isEqualTo("oxia");
        assertThat(config.config()).containsKey("serviceAddress");
    }

    @Test
    void loadEtcdConfig() throws IOException {
        DriverConfig config = DriverConfig.load(Path.of("conf/driver-etcd.yaml"));
        assertThat(config.driver()).isEqualTo("etcd");
        assertThat(config.config()).containsKey("endpoints");
    }

    @Test
    void loadZookeeperConfig() throws IOException {
        DriverConfig config = DriverConfig.load(Path.of("conf/driver-zookeeper.yaml"));
        assertThat(config.driver()).isEqualTo("zookeeper");
        assertThat(config.config()).containsKey("servers");
    }

    @Test
    void missingFileThrows() {
        assertThatThrownBy(() -> DriverConfig.load(Path.of("nonexistent.yaml")))
                .isInstanceOf(IOException.class);
    }

    @Test
    void unknownDriverThrows() {
        DriverConfig config = new DriverConfig("unknown", null, java.util.Map.of());
        assertThatThrownBy(() -> DriverFactory.build(config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown driver");
    }

    @Test
    void labelDefaultsToNull() throws IOException {
        DriverConfig config = DriverConfig.load(Path.of("conf/driver-oxia.yaml"));
        assertThat(config.label()).isNull();
    }

    @Test
    void labelParsedFromYaml(@org.junit.jupiter.api.io.TempDir Path tmp) throws Exception {
        Path f = tmp.resolve("driver.yaml");
        java.nio.file.Files.writeString(
                f,
                """
                driver: redis
                label: redis-testlabel
                config:
                  host: localhost
                  port: 16379
                """);
        DriverConfig config = DriverConfig.load(f);
        assertThat(config.driver()).isEqualTo("redis");
        assertThat(config.label()).isEqualTo("redis-testlabel");
    }
}
