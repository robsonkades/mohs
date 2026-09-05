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

import org.jspecify.annotations.Nullable;

import io.mohs.core.EngineState;

/**
 * A cluster node with the most recent known heartbeat — the internal representation of
 * {@link NodeStore}; the public form is {@code io.mohs.core.NodeSnapshot}, mapped by
 * {@code MohsImpl#nodes} (from which REST derives {@code io.mohs.rest.node.NodeResponse}).
 *
 * <p>{@code epoch} is the node's incarnation (bumped on the node itself when it notices its lease
 * expired); {@code expiresAt} is the NODE's lease, renewed by the heartbeat — {@code null} only on
 * a row written by an older jar (the reaper then falls back to the heartbeat's staleness).
 *
 * @param nodeId the identity of the engine node
 * @param state the current lifecycle state
 * @param lastHeartbeatAt the instant of the last recorded heartbeat
 * @param epoch the ownership generation used by the completion fence
 * @param expiresAt the instant when the liveness lease expires
 */
public record StoredNode(String nodeId, EngineState state, Instant lastHeartbeatAt, long epoch,
        @Nullable Instant expiresAt) {

    /**
     * Creates a {@code StoredNode} with the supplied values.
     *
     * @param nodeId the identity of the engine node
     * @param state the current lifecycle state
     * @param lastHeartbeatAt the instant of the last recorded heartbeat
     * @param epoch the ownership generation used by the completion fence
     * @param expiresAt the instant when the liveness lease expires
     */
    public StoredNode {
        Objects.requireNonNull(nodeId, "nodeId");
        Objects.requireNonNull(state, "state");
        Objects.requireNonNull(lastHeartbeatAt, "lastHeartbeatAt");
    }
}
