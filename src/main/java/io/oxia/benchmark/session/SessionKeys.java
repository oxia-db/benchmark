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
package io.oxia.benchmark.session;

/**
 * Deterministic key namespace for session ephemerals, shared by every backend so the same key
 * strings are used everywhere. Each session {@code id} owns a parent {@code <root>/<id>} holding
 * its ephemeral leaves {@code <root>/<id>/e<j>}. The parent groups a session's keys so a watch
 * scoped to it sees exactly that session's ephemerals (what ZooKeeper's child watch needs; a plain
 * key prefix for Oxia/etcd — the zero-padded fixed-width id keeps prefixes non-overlapping). Keys
 * derive from the id alone, so the kill experiments can reconstruct a departed session's keys
 * without its handle.
 */
public final class SessionKeys {

    private final String root;

    public SessionKeys(String root) {
        this.root = root.replaceAll("^/+|/+$", "");
    }

    /** The parent node grouping session {@code id}'s ephemeral keys (also the watch scope). */
    public String parent(long id) {
        return root + "/" + String.format("%016d", id);
    }

    /** The j-th ephemeral key of session {@code id}. */
    public String ephemeral(long id, int j) {
        return parent(id) + "/e" + j;
    }
}
