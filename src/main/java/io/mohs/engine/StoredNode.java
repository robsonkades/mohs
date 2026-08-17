package io.mohs.engine;

import java.time.Instant;
import java.util.Objects;

import io.mohs.core.EngineState;

/**
 * Um node do cluster com o heartbeat mais recente conhecido (ADR-0012) —
 * representação interna de {@link NodeStore}; a forma pública é
 * {@code io.mohs.core.NodeSnapshot}, mapeada por {@code MohsImpl#nodes}
 * (e dela o REST deriva {@code io.mohs.rest.node.NodeResponse}).
 */
public record StoredNode(String nodeId, EngineState state, Instant lastHeartbeatAt) {

    public StoredNode {
        Objects.requireNonNull(nodeId, "nodeId");
        Objects.requireNonNull(state, "state");
        Objects.requireNonNull(lastHeartbeatAt, "lastHeartbeatAt");
    }
}
