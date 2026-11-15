/*
 * Copyright 2026 The Mohs Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.mohs.engine;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import io.mohs.core.EngineState;

/** Durable sessions. The store arbitrates renewal and irrevocable revocation with its own clock. */
public interface NodeStore {
    /** The identity is known but cannot be renewed or reused. */
    final class SessionExpired extends IllegalStateException {
        private static final long serialVersionUID = 1L;

        public SessionExpired() {
            super("session identity expired; rejoin with a fresh node ID");
        }
    }
    /** Creates a fresh session; repeating an unconfirmed creation with the same ID is safe. */
    StoredNode register(String nodeId, EngineState state, Duration ttl, ExecutableCatalog catalog);
    /** Empty means the session expired or was revoked, and rejoining requires a fresh identity. */
    Optional<StoredNode> renew(String nodeId, EngineState state, Duration ttl);
    /** Revalidates expiration under the same lock as renewal, then irrevocably revokes the session. */
    boolean revokeExpired(String nodeId);
    /** Revokes after graceful draining; outstanding work becomes recoverable. */
    void stop(String nodeId);
    /** Includes revoked sessions awaiting cleanup; lifecycle and validity are separate facts. */
    List<StoredNode> findAll();
    /** Deletes only old revoked sessions without outstanding work. */
    int deleteHeartbeatsBefore(Instant cutoff);
}
