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

import io.oxia.benchmark.driver.consul.ConsulDriver;
import io.oxia.benchmark.driver.etcd.EtcdDriver;
import io.oxia.benchmark.driver.oxia.OxiaDriver;
import io.oxia.benchmark.driver.zookeeper.ZooKeeperDriver;

public final class DriverFactory {

    private DriverFactory() {}

    public static KVStoreDriver build(DriverConfig conf) throws Exception {
        KVStoreDriver driver =
                switch (conf.driver()) {
                    case "oxia" -> new OxiaDriver();
                    case "etcd" -> new EtcdDriver();
                    case "zookeeper" -> new ZooKeeperDriver();
                    case "consul" -> new ConsulDriver();
                    default -> throw new IllegalArgumentException("Unknown driver: " + conf.driver());
                };
        driver.init(conf.config());
        return driver;
    }
}
