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
import io.oxia.benchmark.driver.redis.RedisDriver;
import io.oxia.benchmark.driver.session.SessionDriver;
import io.oxia.benchmark.driver.session.etcd.EtcdSessionDriver;
import io.oxia.benchmark.driver.session.oxia.OxiaSessionDriver;
import io.oxia.benchmark.driver.session.zookeeper.ZooKeeperSessionDriver;
import io.oxia.benchmark.driver.tikv.TiKVDriver;
import io.oxia.benchmark.driver.zookeeper.ZooKeeperDriver;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public final class DriverFactory {

    private DriverFactory() {}

    public static KVStoreDriver build(DriverConfig conf) throws Exception {
        KVStoreDriver driver =
                switch (conf.driver()) {
                    case "oxia" -> new OxiaDriver();
                    case "etcd" -> new EtcdDriver();
                    case "zookeeper" -> new ZooKeeperDriver();
                    case "redis" -> new RedisDriver();
                    case "tikv" -> new TiKVDriver();
                    case "consul" -> new ConsulDriver();
                    default -> throw new IllegalArgumentException("Unknown driver: " + conf.driver());
                };
        driver.init(conf.config());
        if (conf.label() != null && !conf.label().isEmpty()) {
            return new LabeledDriver(driver, conf.label());
        }
        return driver;
    }

    /**
     * Session-capable variant for the session/ephemeral experiments. Only the backends with a real
     * session model are supported; the KV-only ones (Redis, TiKV, Consul) have no session contract.
     * The optional config label is applied to the result by the caller rather than via a wrapper.
     */
    public static SessionDriver buildSession(DriverConfig conf) throws Exception {
        SessionDriver driver =
                switch (conf.driver()) {
                    case "oxia" -> new OxiaSessionDriver();
                    case "etcd" -> new EtcdSessionDriver();
                    case "zookeeper" -> new ZooKeeperSessionDriver();
                    default ->
                            throw new IllegalArgumentException(
                                    "Driver '"
                                            + conf.driver()
                                            + "' has no session/ephemeral model; session experiments support"
                                            + " oxia, etcd, zookeeper");
                };
        driver.init(conf.config());
        return driver;
    }

    /**
     * Delegates to the real driver but reports a custom name, so the same backend can appear under
     * distinct labels in results and metrics (e.g. different cluster sizes).
     */
    private record LabeledDriver(KVStoreDriver delegate, String label) implements KVStoreDriver {
        @Override
        public String name() {
            return label;
        }

        @Override
        public void init(Map<String, Object> config) throws Exception {
            delegate.init(config);
        }

        @Override
        public CompletableFuture<Void> put(String key, byte[] value) {
            return delegate.put(key, value);
        }

        @Override
        public CompletableFuture<Void> get(String key) {
            return delegate.get(key);
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }
}
