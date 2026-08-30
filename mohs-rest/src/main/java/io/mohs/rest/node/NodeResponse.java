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
package io.mohs.rest.node;

import java.time.Instant;
import java.util.Objects;

import io.mohs.core.EngineState;
import io.mohs.core.NodeSnapshot;

/** A cluster node with a recent heartbeat — a read over the heartbeat registry that liveness builds. */
public record NodeResponse(String nodeId, EngineState state, Instant lastHeartbeatAt) {

    public NodeResponse {
        Objects.requireNonNull(nodeId, "nodeId");
        Objects.requireNonNull(state, "state");
        Objects.requireNonNull(lastHeartbeatAt, "lastHeartbeatAt");
    }

    public static NodeResponse from(NodeSnapshot node) {
        return new NodeResponse(node.nodeId(), node.state(), node.lastHeartbeatAt());
    }
}
