package io.mohs.rest;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import io.mohs.core.execution.ExecutionState;

/**
 * Âncora de polling do dashboard: barato por construção — contagens por
 * status, profundidade por queue, throughput da janela recente (ver
 * {@code docs/REST-API-DESIGN.md}, {@code GET /overview}).
 */
public record OverviewResponse(
        Map<ExecutionState, Long> executionCountsByStatus,
        List<QueueDepthView> queueDepths,
        ThroughputView throughput) {

    public OverviewResponse {
        Objects.requireNonNull(executionCountsByStatus, "executionCountsByStatus");
        Objects.requireNonNull(queueDepths, "queueDepths");
        Objects.requireNonNull(throughput, "throughput");
        executionCountsByStatus = Map.copyOf(executionCountsByStatus);
        queueDepths = List.copyOf(queueDepths);
    }
}
