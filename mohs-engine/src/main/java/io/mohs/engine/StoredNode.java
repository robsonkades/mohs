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

import java.time.Instant;
import java.util.Objects;
import io.mohs.core.EngineState;

/** A durable session snapshot; lifecycle state is independent of irrevocable revocation. */
public record StoredNode(String nodeId, EngineState state, Instant lastHeartbeatAt,
        Instant expiresAt, boolean revoked) {
    public StoredNode {
        io.mohs.core.GeneratedId.require(nodeId, "nodeId");
        Objects.requireNonNull(state, "state");
        Objects.requireNonNull(lastHeartbeatAt, "lastHeartbeatAt");
        Objects.requireNonNull(expiresAt, "expiresAt");
    }

    /** Read-side estimate; the store rechecks validity when granting or completing work. */
    public boolean validAt(Instant now) {
        return !revoked && expiresAt.isAfter(now);
    }
}
