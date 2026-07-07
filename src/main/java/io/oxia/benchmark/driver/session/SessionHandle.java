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
 * Opaque handle to a live session (Oxia session / ZooKeeper connection / etcd lease). Created by
 * {@link SessionDriver#createSession}, it is the token passed back to attach ephemeral keys and to
 * close or kill the session. Implementations subclass this to stash their backend state (client,
 * lease id, znode parent), so the runner and pool stay backend-agnostic.
 */
public abstract class SessionHandle {

    private final long logicalId;

    protected SessionHandle(long logicalId) {
        this.logicalId = logicalId;
    }

    /** The pool-assigned logical id, unique within a worker; drives this session's key namespace. */
    public final long logicalId() {
        return logicalId;
    }

    /** The backend-native session/lease id if known, else -1. For logging/diagnostics only. */
    public long backendId() {
        return -1;
    }
}
