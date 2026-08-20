package io.mohs.core.execution;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

import io.mohs.core.job.JobKey;
import io.mohs.core.definition.JobDefinition;

import org.jspecify.annotations.Nullable;

/**
 * Um único disparo de um {@link JobDefinition}: identidade, estado atual,
 * o actor que causou o disparo e cada {@link Attempt} feita até agora. A
 * trilha de actor é inegociável em toda invocação (ver
 * {@code docs/API-DESIGN.md} §"Actor e regressão ergonômica assumida").
 * {@code firedAt} é {@code null} enquanto a execução ainda não disparou
 * (ex.: estado {@link ExecutionState#ENQUEUED}).
 */
public record Execution(
        ExecutionId id,
        JobKey jobKey,
        ExecutionState state,
        Instant scheduledAt,
        @Nullable Instant firedAt,
        List<Attempt> attempts,
        String actor,
        Priority priority,
        @Nullable String idempotencyKey,
        @Nullable String batchId) {

    /**
     * O actor das ocorrências materializadas pelo trigger recorrente
     * (ADR-0035) — como se distingue um disparo do próprio motor de um
     * agendamento manual/programático. Nome <b>reservado</b>: além de
     * trilha de auditoria, ele carrega decisão de motor (o rearme da
     * corrente fixed-delay só acontece em ocorrência do scheduler), então
     * {@code ScheduleCommand.as} e a API REST o rejeitam — um agendamento
     * manual jamais pode se passar pelo motor.
     */
    public static final String SCHEDULER_ACTOR = "scheduler";

    public Execution {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(jobKey, "jobKey");
        Objects.requireNonNull(state, "state");
        Objects.requireNonNull(scheduledAt, "scheduledAt");
        Objects.requireNonNull(actor, "actor");
        if (actor.isBlank()) {
            throw new IllegalArgumentException("actor must not be blank");
        }
        Objects.requireNonNull(priority, "priority");
        attempts = List.copyOf(attempts); // cópia defensiva (Effective Java, Item 50)
    }

    /** {@link Priority#NORMAL}, sem {@code idempotencyKey} — mesmo default do schema (`DEFAULT 20`). */
    public Execution(ExecutionId id, JobKey jobKey, ExecutionState state, Instant scheduledAt,
            @Nullable Instant firedAt, List<Attempt> attempts, String actor) {
        this(id, jobKey, state, scheduledAt, firedAt, attempts, actor, Priority.NORMAL, null, null);
    }

    /**
     * Fora de lote, que é o caso da esmagadora maioria das execuções —
     * {@code batchId} nulo. Existe para o agendamento avulso não ter que
     * dizer "não pertenço a lote nenhum" em toda chamada, e porque
     * {@code batchId} entrou depois (ADR-0043): sem esta forma, todo
     * construtor posicional já escrito quebraria para declarar uma ausência.
     */
    public Execution(ExecutionId id, JobKey jobKey, ExecutionState state, Instant scheduledAt,
            @Nullable Instant firedAt, List<Attempt> attempts, String actor, Priority priority,
            @Nullable String idempotencyKey) {
        this(id, jobKey, state, scheduledAt, firedAt, attempts, actor, priority, idempotencyKey, null);
    }
}
