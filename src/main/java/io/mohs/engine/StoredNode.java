package io.mohs.engine;

import java.time.Instant;
import java.util.Objects;

import io.mohs.core.EngineState;

/**
 * Um node do cluster com o heartbeat mais recente conhecido (ADR-0012) —
 * representação interna de {@link NodeStore}; {@code io.mohs.rest.node.NodeResponse}
 * é o DTO de fora, mapeado a partir deste tipo por quem ligar {@code
 * NodesController} a um {@link NodeStore} de verdade (fora do escopo
 * desta etapa).
 */
public record StoredNode(String nodeId, EngineState state, Instant lastHeartbeatAt) {

    public StoredNode {
        Objects.requireNonNull(nodeId, "nodeId");
        Objects.requireNonNull(state, "state");
        Objects.requireNonNull(lastHeartbeatAt, "lastHeartbeatAt");
    }
}
