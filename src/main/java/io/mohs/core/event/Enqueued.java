package io.mohs.core.event;

import java.time.Instant;
import java.util.Objects;

import io.mohs.core.execution.ExecutionId;
import io.mohs.core.job.JobKey;
import io.mohs.core.ScheduleCommand;

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
