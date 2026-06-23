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
package io.oxia.benchmark.driver.redis;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import io.oxia.benchmark.driver.KVStoreDriver;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.CustomLog;

/** Driver for Redis, using the async Lettuce client (string keys, raw byte[] values). */
@CustomLog
public class RedisDriver implements KVStoreDriver {

    private RedisClient client;
    private StatefulRedisConnection<String, byte[]> connection;
    private RedisAsyncCommands<String, byte[]> commands;

    @Override
    public String name() {
        return "redis";
    }

    @Override
    public void init(Map<String, Object> config) {
        log.info().attr("config", config).log("Initializing Redis driver");

        String address = (String) config.getOrDefault("address", "localhost:6379");
        client = RedisClient.create(RedisURI.create("redis://" + address));
        connection = client.connect(RedisCodec.of(StringCodec.UTF8, ByteArrayCodec.INSTANCE));
        commands = connection.async();
    }

    @Override
    public CompletableFuture<Void> put(String key, byte[] value) {
        return commands.set(key, value).toCompletableFuture().thenApply(r -> null);
    }

    @Override
    public CompletableFuture<Void> get(String key) {
        return commands.get(key).toCompletableFuture().thenApply(r -> null);
    }

    @Override
    public void close() {
        if (connection != null) {
            connection.close();
        }
        if (client != null) {
            client.shutdown();
        }
    }
}
