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
package io.mohs.core;

import java.time.Instant;
import java.util.Objects;

/**
 * A cluster node as seen through the heartbeat registry: its identity, the state it self-reported
 * on the last tick, and when it was last seen.
 *
 * <p>Death is deliberately NOT a field — a crash writes nothing. Alive versus suspect is derived
 * from the age of {@code lastHeartbeatAt} at read time, {@code STOPPED} is the only self-reported
 * outcome (a clean shutdown), and the purge keeps this list to recent nodes.
 *
 * @param nodeId the identity of the engine node
 * @param state the current lifecycle state
 * @param lastHeartbeatAt the instant of the last recorded heartbeat
 */
public record NodeSnapshot(String nodeId, EngineState state, Instant lastHeartbeatAt) {

    /**
     * Creates a {@code NodeSnapshot} with the supplied values.
     *
     * @param nodeId the identity of the engine node
     * @param state the current lifecycle state
     * @param lastHeartbeatAt the instant of the last recorded heartbeat
     */
    public NodeSnapshot {
        Objects.requireNonNull(nodeId, "nodeId");
        Objects.requireNonNull(state, "state");
        Objects.requireNonNull(lastHeartbeatAt, "lastHeartbeatAt");
    }
}
