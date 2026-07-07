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
 * scoped to it sees exactly that session's ephemerals (which is what ZooKeeper's child watch needs,
 * and a plain key prefix for Oxia/etcd). Keys derive from the id alone, so the kill experiments can
 * reconstruct a departed session's keys without holding its handle.
 */
public final class SessionKeys {

    private final String root;

    public SessionKeys(String root) {
        // Normalize: no leading/trailing slash internally; callers get slash-joined paths.
        String r = root;
        while (r.startsWith("/")) {
            r = r.substring(1);
        }
        while (r.endsWith("/")) {
            r = r.substring(0, r.length() - 1);
        }
        this.root = r;
    }

    /** The prefix covering every session's keys under this namespace (trailing slash included). */
    public String prefix() {
        return root + "/";
    }

    /** The parent node grouping session {@code id}'s ephemeral keys (also the watch scope). */
    public String parent(long id) {
        return root + "/" + pad(id);
    }

    /** The j-th ephemeral key of session {@code id}. */
    public String ephemeral(long id, int j) {
        return parent(id) + "/e" + j;
    }

    private static String pad(long id) {
        String s = Long.toString(id);
        int width = 16;
        if (s.length() >= width) {
            return s;
        }
        StringBuilder sb = new StringBuilder(width);
        for (int i = s.length(); i < width; i++) {
            sb.append('0');
        }
        return sb.append(s).toString();
    }
}
