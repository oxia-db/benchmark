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

/**
 * Callback for {@link SessionDriver#watchPrefix} — the native change feed of each system,
 * normalized to key create/delete events. The cleanup-visibility experiment (S3) cares about
 * deletions: when a session expires, the server tombstones its ephemeral keys and the change feed
 * reports each removal. {@code atNanos} is the observer's {@link System#nanoTime()} at delivery, so
 * latency is measured on one clock.
 */
public interface PrefixListener {

    /** A key under the watched prefix was deleted (e.g. ephemeral removed on session expiry). */
    void onKeyDeleted(String key, long atNanos);

    /** A key under the watched prefix was created. Most experiments ignore this. */
    default void onKeyCreated(String key, long atNanos) {}
}
