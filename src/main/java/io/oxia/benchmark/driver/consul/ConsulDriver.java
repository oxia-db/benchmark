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
package io.oxia.benchmark.driver.consul;

import io.oxia.benchmark.driver.KVStoreDriver;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse.BodyHandlers;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.CustomLog;

/**
 * Driver for HashiCorp Consul's KV store. Consul's API is HTTP, so this uses the JDK's async {@link
 * HttpClient} directly rather than a third-party client.
 */
@CustomLog
public class ConsulDriver implements KVStoreDriver {

    // Consul KV keys must not start with '/'; namespace benchmark keys under "test/".
    private static final String KEY_PREFIX = "test/";

    private HttpClient client;
    private String kvBaseUrl;

    @Override
    public String name() {
        return "consul";
    }

    @Override
    public void init(Map<String, Object> config) {
        log.info().attr("config", config).log("Initializing Consul driver");

        String address = (String) config.getOrDefault("address", "localhost:8500");
        kvBaseUrl = "http://" + address + "/v1/kv/" + KEY_PREFIX;

        client =
                HttpClient.newBuilder()
                        .version(HttpClient.Version.HTTP_1_1)
                        .connectTimeout(Duration.ofSeconds(5))
                        .build();
    }

    @Override
    public CompletableFuture<Void> put(String key, byte[] value) {
        HttpRequest request =
                HttpRequest.newBuilder(URI.create(kvBaseUrl + key))
                        .timeout(Duration.ofSeconds(10))
                        .PUT(BodyPublishers.ofByteArray(value))
                        .build();
        return client
                .sendAsync(request, BodyHandlers.discarding())
                .thenApply(
                        response -> {
                            if (response.statusCode() != 200) {
                                throw new IllegalStateException(
                                        "Consul PUT failed with status " + response.statusCode());
                            }
                            return null;
                        });
    }

    @Override
    public CompletableFuture<Void> get(String key) {
        HttpRequest request =
                HttpRequest.newBuilder(URI.create(kvBaseUrl + key + "?raw"))
                        .timeout(Duration.ofSeconds(10))
                        .GET()
                        .build();
        // 200 = found, 404 = absent; both are valid benchmark outcomes, matching the other drivers.
        return client.sendAsync(request, BodyHandlers.discarding()).thenApply(response -> null);
    }

    @Override
    public void close() {
        // java.net.http.HttpClient is not Closeable on JDK 17; the JVM reclaims it on exit.
    }
}
