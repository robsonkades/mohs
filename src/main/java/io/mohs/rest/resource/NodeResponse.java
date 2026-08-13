package io.mohs.rest.resource;

import java.time.Instant;
import java.util.Objects;

import io.mohs.core.EngineState;

/** Nó do cluster com heartbeat recente — leitura sobre o registro de heartbeat que a liveness (M3) constrói. */
public record NodeResponse(String nodeId, EngineState state, Instant lastHeartbeatAt) {

    public NodeResponse {
        Objects.requireNonNull(nodeId, "nodeId");
        Objects.requireNonNull(state, "state");
        Objects.requireNonNull(lastHeartbeatAt, "lastHeartbeatAt");
    }
}
