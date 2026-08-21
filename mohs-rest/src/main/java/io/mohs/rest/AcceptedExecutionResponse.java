package io.mohs.rest;

import java.time.Instant;
import java.util.Objects;

import io.mohs.core.event.Enqueued;
import io.mohs.core.execution.Execution;

/**
 * Recibo `202 Accepted` de toda invocação (schedule, retry) — a forma
 * plana usada na borda HTTP do {@link Enqueued}: identidades cruzam como
 * {@code String}, nunca {@code JobKey}/{@code ExecutionId} diretamente
 * (ver {@code docs/REST-API-DESIGN.md}, exemplo de {@code POST .../schedule}).
 */
public record AcceptedExecutionResponse(String executionId, String jobKey, Instant scheduledAt, String actor) {

    public AcceptedExecutionResponse {
        Objects.requireNonNull(executionId, "executionId");
        Objects.requireNonNull(jobKey, "jobKey");
        Objects.requireNonNull(scheduledAt, "scheduledAt");
        Objects.requireNonNull(actor, "actor");
    }

    public static AcceptedExecutionResponse from(Enqueued enqueued) {
        return new AcceptedExecutionResponse(
                enqueued.executionId().value(), enqueued.jobKey().value(), enqueued.scheduledAt(), enqueued.actor());
    }

    /** A forma 202 do retry manual — a MESMA execução rearmada; o actor é o da invocação original (nada novo é inserido, ADR-0033). */
    public static AcceptedExecutionResponse from(Execution execution) {
        return new AcceptedExecutionResponse(
                execution.id().value(), execution.jobKey().value(), execution.scheduledAt(), execution.actor());
    }
}
