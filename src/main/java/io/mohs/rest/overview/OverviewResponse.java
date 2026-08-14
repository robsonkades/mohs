package io.mohs.rest.overview;

import java.util.Map;
import java.util.Objects;

import io.mohs.core.execution.ExecutionState;

/**
 * Âncora de polling do dashboard: barato por construção — contagens por
 * status, throughput da janela recente (ver
 * {@code docs/REST-API-DESIGN.md}, {@code GET /overview}).
 */
public record OverviewResponse(Map<ExecutionState, Long> executionCountsByStatus, ThroughputView throughput) {

    public OverviewResponse {
        Objects.requireNonNull(executionCountsByStatus, "executionCountsByStatus");
        Objects.requireNonNull(throughput, "throughput");
        executionCountsByStatus = Map.copyOf(executionCountsByStatus);
    }
}
