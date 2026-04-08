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
package io.oxia.benchmark.driver.oxia;

import io.oxia.benchmark.driver.KVStoreDriver;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.OxiaClientBuilder;
import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class OxiaDriver implements KVStoreDriver {

    private static final Logger log = LogManager.getLogger(OxiaDriver.class);

    private AsyncOxiaClient client;

    @Override
    public String name() {
        return "oxia";
    }

    @Override
    public void init(Map<String, Object> config) throws Exception {
        log.info("Initializing Oxia driver with config: {}", config);

        String serviceAddress = (String) config.getOrDefault("serviceAddress", "localhost:6648");
        OxiaClientBuilder builder = OxiaClientBuilder.create(serviceAddress);

        if (config.containsKey("namespace")) {
            builder.namespace((String) config.get("namespace"));
        }
        if (config.containsKey("batchLinger")) {
            Duration batchLinger = parseDuration((String) config.get("batchLinger"));
            builder.batchLinger(batchLinger);
        }
        if (config.containsKey("batchMaxCount")) {
            int batchMaxCount = ((Number) config.get("batchMaxCount")).intValue();
            builder.maxRequestsPerBatch(batchMaxCount);
        }

        client = builder.asyncClient().get();
    }

    @Override
    public CompletableFuture<Void> put(String key, byte[] value) {
        return client.put(key, value).thenApply(r -> null);
    }

    @Override
    public CompletableFuture<Void> get(String key) {
        // get() returns null when key not found, no exception thrown
        return client.get(key).thenApply(r -> null);
    }

    @Override
    public void close() throws IOException {
        if (client != null) {
            try {
                client.close();
            } catch (Exception e) {
                throw new IOException(e);
            }
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
