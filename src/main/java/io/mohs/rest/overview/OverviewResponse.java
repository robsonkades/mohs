package io.mohs.rest.overview;

import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;

import io.mohs.core.OverviewSnapshot;
import io.mohs.core.execution.ExecutionState;

/**
 * Âncora de polling do dashboard: barato por construção — contagens por
 * status, throughput da janela recente (ver
 * {@code docs/REST-API-DESIGN.md}, {@code GET /overview}). Forma plana de
 * {@link OverviewSnapshot}, que é quem define o contrato: só os estados
 * vivos aparecem no mapa (sempre os três, zero incluso) — a atividade
 * terminal recente está em {@link ThroughputView}.
 */
public record OverviewResponse(Map<ExecutionState, Long> executionCountsByStatus, ThroughputView throughput) {

    public OverviewResponse {
        Objects.requireNonNull(executionCountsByStatus, "executionCountsByStatus");
        Objects.requireNonNull(throughput, "throughput");
        // EnumMap + unmodifiableMap, não Map.copyOf: preserva a ordem de
        // declaração do enum no JSON — ordem exposta em API é contrato.
        Map<ExecutionState, Long> ordered = new EnumMap<>(ExecutionState.class);
        ordered.putAll(executionCountsByStatus);
        executionCountsByStatus = Collections.unmodifiableMap(ordered);
    }

    static OverviewResponse from(OverviewSnapshot snapshot) {
        return new OverviewResponse(snapshot.executionCountsByState(),
                new ThroughputView(snapshot.throughputWindow(), snapshot.succeededInWindow(), snapshot.failedInWindow()));
    }
}
