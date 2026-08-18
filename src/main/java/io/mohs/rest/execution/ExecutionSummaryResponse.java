package io.mohs.rest.execution;

import java.time.Instant;
import java.util.Objects;

import org.jspecify.annotations.Nullable;

import io.mohs.core.execution.Execution;
import io.mohs.core.execution.ExecutionState;

/**
 * Forma de LISTA de {@link Execution} — sumário sem {@code attempts}, que
 * pertencem ao detalhe ({@code GET /executions/{id}},
 * {@link ExecutionResponse}), como a tabela do design REST sempre disse
 * (v0.9 só alinhou o wire). O que a lista deixa de carregar não é só
 * volume: {@code error} de attempt é texto de tamanho arbitrário que
 * viajava em toda página e em todo tick do stream do dashboard.
 */
public record ExecutionSummaryResponse(
        String executionId,
        String jobKey,
        ExecutionState state,
        Instant scheduledAt,
        @Nullable Instant firedAt,
        String actor) {

    public ExecutionSummaryResponse {
        Objects.requireNonNull(executionId, "executionId");
        Objects.requireNonNull(jobKey, "jobKey");
        Objects.requireNonNull(state, "state");
        Objects.requireNonNull(scheduledAt, "scheduledAt");
        Objects.requireNonNull(actor, "actor");
    }

    public static ExecutionSummaryResponse from(Execution execution) {
        return new ExecutionSummaryResponse(
                execution.id().value(),
                execution.jobKey().value(),
                execution.state(),
                execution.scheduledAt(),
                execution.firedAt(),
                execution.actor());
    }
}
