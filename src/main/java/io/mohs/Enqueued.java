package io.mohs;

import java.time.Instant;
import java.util.Objects;

/**
 * Dupla função deliberada: é tanto o recibo retornado pelos terminais de
 * {@link ScheduleCommand} ({@code now/at/after}) quanto a variante de
 * {@link ExecutionEvent} correspondente — "retorno é recibo, nunca
 * {@code Future} do resultado" já descreve os dois papéis com o mesmo dado.
 */
public record Enqueued(ExecutionId executionId, JobKey jobKey, Instant scheduledAt, String actor)
        implements ExecutionEvent {

    public Enqueued {
        Objects.requireNonNull(executionId, "executionId");
        Objects.requireNonNull(jobKey, "jobKey");
        Objects.requireNonNull(scheduledAt, "scheduledAt");
        Objects.requireNonNull(actor, "actor");
    }
}
