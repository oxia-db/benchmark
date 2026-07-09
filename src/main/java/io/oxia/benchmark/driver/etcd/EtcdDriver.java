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
package io.oxia.benchmark.driver.etcd;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.oxia.benchmark.driver.KVStoreDriver;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.CustomLog;

@CustomLog
public class EtcdDriver implements KVStoreDriver {

    private Client client;
    private KV kvClient;
    private ExecutorService callbackExecutor;

    @Override
    public String name() {
        return "etcd";
    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(Map<String, Object> config) throws Exception {
        log.info().attr("config", config).log("Initializing etcd driver");

        Object endpointsObj = config.get("endpoints");
        if (endpointsObj == null) {
            throw new IllegalArgumentException("Missing 'endpoints' in etcd config");
        }

        List<String> endpoints = new ArrayList<>();
        if (endpointsObj instanceof List<?> list) {
            for (Object e : list) {
                endpoints.add(e.toString());
            }
        } else {
            throw new IllegalArgumentException(
                    "Invalid type for 'endpoints': " + endpointsObj.getClass());
        }

        String[] targets = endpoints.stream().map(e -> "http://" + e).toArray(String[]::new);

        // connectTimeout is intentionally omitted: tikv-client-java bundles an older, unrelocated
        // io.etcd.jetcd, and the shaded jar may load that ClientBuilder (which lacks
        // connectTimeout). Using only jetcd's stable core builder API keeps the etcd driver robust
        // to whichever copy wins. (dialTimeout config is currently ignored as a result.)
        // jetcd defaults to an unbounded cached thread pool for its async callbacks; under a
        // max-throughput run (thousands of in-flight requests) that grows to thousands of threads
        // and OOMs the worker. Pin it to a bounded pool. executorService() is part of jetcd's
        // stable core builder API (unlike connectTimeout above), so it is safe against the shaded-
        // jetcd ambiguity noted here.
        callbackExecutor = Executors.newFixedThreadPool(64);
        client = Client.builder().endpoints(targets).executorService(callbackExecutor).build();
        kvClient = client.getKVClient();
    }

    @Override
    public CompletableFuture<Void> put(String key, byte[] value) {
        return kvClient
                .put(ByteSequence.from(key, StandardCharsets.UTF_8), ByteSequence.from(value))
                .thenApply(r -> null);
    }

    @Override
    public CompletableFuture<Void> get(String key) {
        return kvClient.get(ByteSequence.from(key, StandardCharsets.UTF_8)).thenApply(r -> null);
    }

    @Override
    public void close() throws IOException {
        if (kvClient != null) {
            kvClient.close();
        }
        if (client != null) {
            client.close();
        }
        if (callbackExecutor != null) {
            callbackExecutor.shutdownNow();
        }
    }
}
