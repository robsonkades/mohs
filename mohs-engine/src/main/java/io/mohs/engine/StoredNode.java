package io.mohs.engine;

import java.time.Instant;
import java.util.Objects;

import org.jspecify.annotations.Nullable;

import io.mohs.core.EngineState;

/**
 * Um node do cluster com o heartbeat mais recente conhecido (ADR-0012,
 * promovido a autoridade de liveness pela ADR-0051) — representação
 * interna de {@link NodeStore}; a forma pública é
 * {@code io.mohs.core.NodeSnapshot}, mapeada por {@code MohsImpl#nodes}
 * (e dela o REST deriva {@code io.mohs.rest.node.NodeResponse}).
 *
 * <p>{@code epoch} é a encarnação do node (bump no próprio node quando ele
 * percebe o lease expirado — §11.2 do redesign); {@code expiresAt} é o
 * lease do NÓ, renovado pelo heartbeat — {@code null} só em linha gravada
 * por jar pré-Phase-4 (o reaper trata pela staleness do heartbeat,
 * ADR-0012).
 */
public record StoredNode(String nodeId, EngineState state, Instant lastHeartbeatAt, long epoch,
        @Nullable Instant expiresAt) {

    public StoredNode {
        Objects.requireNonNull(nodeId, "nodeId");
        Objects.requireNonNull(state, "state");
        Objects.requireNonNull(lastHeartbeatAt, "lastHeartbeatAt");
    }
}
