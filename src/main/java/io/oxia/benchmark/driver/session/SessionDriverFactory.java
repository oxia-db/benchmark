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
package io.oxia.benchmark.driver.session;

import io.oxia.benchmark.driver.DriverConfig;
import io.oxia.benchmark.driver.session.etcd.EtcdSessionDriver;
import io.oxia.benchmark.driver.session.oxia.OxiaSessionDriver;
import io.oxia.benchmark.driver.session.zookeeper.ZooKeeperSessionDriver;

/**
 * Builds the session-capable driver for a backend. Only the three systems with a real session /
 * ephemeral model (Oxia, ZooKeeper, etcd) are supported; the KV-only backends (Redis, TiKV, Consul)
 * have no session contract and are intentionally absent. The returned driver's {@code name()}
 * honors the optional {@link DriverConfig#label()} so the same backend can appear under distinct
 * labels.
 */
public final class SessionDriverFactory {

    private SessionDriverFactory() {}

    public static SessionDriver build(DriverConfig conf) throws Exception {
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
        String label = conf.label();
        return label != null && !label.isEmpty() ? new LabeledSessionDriver(driver, label) : driver;
    }

    /** Delegates to the real driver but reports a custom name (e.g. "oxia-3" vs "oxia-12"). */
    private record LabeledSessionDriver(SessionDriver delegate, String label)
            implements SessionDriver {
        @Override
        public String name() {
            return label;
        }

        @Override
        public void init(java.util.Map<String, Object> config) throws Exception {
            delegate.init(config);
        }

        @Override
        public java.util.concurrent.CompletableFuture<Void> put(String key, byte[] value) {
            return delegate.put(key, value);
        }

        @Override
        public java.util.concurrent.CompletableFuture<Void> get(String key) {
            return delegate.get(key);
        }

        @Override
        public java.util.concurrent.CompletableFuture<SessionHandle> createSession(
                long logicalId, java.time.Duration timeout) {
            return delegate.createSession(logicalId, timeout);
        }

        @Override
        public java.util.concurrent.CompletableFuture<Void> putEphemeral(
                SessionHandle session, String key, byte[] value) {
            return delegate.putEphemeral(session, key, value);
        }

        @Override
        public java.util.concurrent.CompletableFuture<Void> closeSession(SessionHandle session) {
            return delegate.closeSession(session);
        }

        @Override
        public java.util.concurrent.CompletableFuture<Void> killSession(SessionHandle session) {
            return delegate.killSession(session);
        }

        @Override
        public java.util.concurrent.CompletableFuture<Boolean> exists(String key) {
            return delegate.exists(key);
        }

        @Override
        public java.io.Closeable watchPrefix(String prefix, PrefixListener listener) {
            return delegate.watchPrefix(prefix, listener);
        }

        @Override
        public void close() throws java.io.IOException {
            delegate.close();
        }
    }
}
