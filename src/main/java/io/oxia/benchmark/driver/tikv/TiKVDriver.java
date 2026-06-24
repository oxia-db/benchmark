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
package io.oxia.benchmark.driver.tikv;

import io.oxia.benchmark.driver.KVStoreDriver;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.CustomLog;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.raw.RawKVClient;
import org.tikv.shade.com.google.protobuf.ByteString;

/**
 * Driver for TiKV's raw KV API. The TiKV Java client is synchronous, so calls are bridged to {@link
 * CompletableFuture} on a bounded thread pool (pool size bounds the in-flight concurrency).
 */
@CustomLog
public class TiKVDriver implements KVStoreDriver {

    private TiSession session;
    private RawKVClient client;
    private ExecutorService executor;

    @Override
    public String name() {
        return "tikv";
    }

    @Override
    public void init(Map<String, Object> config) {
        log.info().attr("config", config).log("Initializing TiKV driver");

        String pdAddresses = (String) config.getOrDefault("pdAddresses", "localhost:2379");
        int threads = config.containsKey("threads") ? ((Number) config.get("threads")).intValue() : 64;

        TiConfiguration conf = TiConfiguration.createRawDefault(pdAddresses);
        // The client defaults (200ms per-RPC deadline, 2s raw-op budget) are too tight for a
        // resource-constrained single-node store (e.g. on kind): slow responses trip
        // DEADLINE_EXCEEDED and the operation fails with "retry is exhausted". Generous timeouts
        // let it complete; on a healthy cluster responses are sub-millisecond so these never bite.
        conf.setTimeout(5000);
        conf.setForwardTimeout(5000);
        conf.setRawKVReadTimeoutInMS(10000);
        conf.setRawKVWriteTimeoutInMS(10000);
        session = TiSession.create(conf);
        client = session.createRawClient();
        executor = Executors.newFixedThreadPool(threads);
    }

    @Override
    public CompletableFuture<Void> put(String key, byte[] value) {
        return CompletableFuture.runAsync(
                () -> client.put(ByteString.copyFromUtf8(key), ByteString.copyFrom(value)), executor);
    }

    @Override
    public CompletableFuture<Void> get(String key) {
        return CompletableFuture.runAsync(() -> client.get(ByteString.copyFromUtf8(key)), executor);
    }

    @Override
    public void close() throws IOException {
        if (executor != null) {
            executor.shutdown();
        }
        if (client != null) {
            client.close();
        }
        if (session != null) {
            try {
                session.close();
            } catch (Exception e) {
                throw new IOException(e);
            }
        }
    }
}
