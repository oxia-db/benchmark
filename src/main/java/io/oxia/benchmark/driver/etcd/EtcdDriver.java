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
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class EtcdDriver implements KVStoreDriver {

    private static final Logger log = LogManager.getLogger(EtcdDriver.class);

    private Client client;
    private KV kvClient;

    @Override
    public String name() {
        return "etcd";
    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(Map<String, Object> config) throws Exception {
        log.info("Initializing etcd driver with config: {}", config);

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

        Duration timeout = Duration.ofSeconds(5);
        if (config.containsKey("dialTimeout")) {
            Object t = config.get("dialTimeout");
            if (t instanceof String s) {
                timeout = parseDuration(s);
            } else if (t instanceof Number n) {
                timeout = Duration.ofSeconds(n.longValue());
            }
        }

        String[] targets = endpoints.stream().map(e -> "http://" + e).toArray(String[]::new);

        client = Client.builder().endpoints(targets).connectTimeout(timeout).build();
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
    }

    private static Duration parseDuration(String s) {
        if (s.endsWith("ms")) {
            return Duration.ofMillis(Long.parseLong(s.substring(0, s.length() - 2)));
        }
        if (s.endsWith("s")) {
            return Duration.ofSeconds(Long.parseLong(s.substring(0, s.length() - 1)));
        }
        return Duration.parse("PT" + s);
    }
}
