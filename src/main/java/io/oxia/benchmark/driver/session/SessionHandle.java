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
 * Opaque handle to a live session (Oxia session / ZooKeeper connection / etcd lease), returned by
 * {@link SessionDriver#createSession} and passed back to attach ephemeral keys and to close or kill
 * the session. Drivers implement it with a record carrying their backend state (client, lease id),
 * so the runner and pool stay backend-agnostic.
 */
public interface SessionHandle {

    /** The pool-assigned logical id, unique within a worker; drives this session's key namespace. */
    long logicalId();
}
