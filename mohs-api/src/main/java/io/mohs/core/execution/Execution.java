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
 * {@code firedAt} é o instante em que a execução foi reivindicada por um
 * node e fica {@code null} enquanto isso não ocorreu (ex.: estado
 * {@link ExecutionState#ENQUEUED}); desde a Phase 5 do redesign (posse em
 * {@code mohs_lease}) ele também volta a {@code null} DEPOIS do término —
 * a pergunta histórica "quando cada attempt começou" é de
 * {@link Attempt#startedAt}. {@code owner} (§16.3-2 do redesign) responde
 * "QUEM está executando isto agora": o {@code node_id} dono da posse
 * enquanto {@link ExecutionState#RUNNING}, {@code null} fora disso.
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
        @Nullable String batchId,
        @Nullable String owner) {

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

    /** {@link Priority#NORMAL}, sem {@code idempotencyKey}, fora de lote e sem dono corrente — mesmo default do schema (`DEFAULT 20`). */
    public Execution(ExecutionId id, JobKey jobKey, ExecutionState state, Instant scheduledAt,
            @Nullable Instant firedAt, List<Attempt> attempts, String actor) {
        this(id, jobKey, state, scheduledAt, firedAt, attempts, actor, Priority.NORMAL, null, null, null);
    }

    /**
     * Fora de lote, que é o caso da esmagadora maioria das execuções: só o
     * agendamento em lote preenche {@code batchId} (ADR-0043). Existe para o
     * agendamento avulso não ter que dizer "não pertenço a lote nenhum" em
     * toda chamada.
     */
    public Execution(ExecutionId id, JobKey jobKey, ExecutionState state, Instant scheduledAt,
            @Nullable Instant firedAt, List<Attempt> attempts, String actor, Priority priority,
            @Nullable String idempotencyKey) {
        this(id, jobKey, state, scheduledAt, firedAt, attempts, actor, priority, idempotencyKey, null, null);
    }

    /** Sem dono corrente — a forma da era pré-split, preservada pra quem constrói leituras históricas (o dono só existe em {@code RUNNING}). */
    public Execution(ExecutionId id, JobKey jobKey, ExecutionState state, Instant scheduledAt,
            @Nullable Instant firedAt, List<Attempt> attempts, String actor, Priority priority,
            @Nullable String idempotencyKey, @Nullable String batchId) {
        this(id, jobKey, state, scheduledAt, firedAt, attempts, actor, priority, idempotencyKey, batchId, null);
    }
}
