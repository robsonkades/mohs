package io.mohs.engine;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

import org.jspecify.annotations.Nullable;
import org.springframework.dao.DuplicateKeyException;

import io.github.robsonkades.uuidv7.UUIDv7;

import io.mohs.core.ScheduleCommand;
import io.mohs.core.event.Enqueued;
import io.mohs.core.execution.Execution;
import io.mohs.core.execution.ExecutionId;
import io.mohs.core.execution.ExecutionState;
import io.mohs.core.execution.Priority;
import io.mohs.core.job.JobKey;

/**
 * {@link ScheduleCommand} sobre {@link JobStore}/{@link ExecutionStore} —
 * acumula {@code priority}/{@code actor}/{@code idempotencyKey} até um
 * terminal ({@code now}/{@code at}/{@code after}) gravar a execução; uma
 * cadeia abandonada antes do terminal nunca toca o banco (mesmo contrato
 * documentado por {@code @CheckReturnValue} na interface).
 */
final class ScheduleCommandImpl implements ScheduleCommand {

    private final JobStore jobStore;
    private final ExecutionStore executionStore;
    private final Clock clock;
    private final JobKey jobKey;
    private final Object payload;

    private Priority priority = Priority.NORMAL;
    private String actor = MohsImpl.DEFAULT_ACTOR;
    private @Nullable String idempotencyKey;

    ScheduleCommandImpl(JobStore jobStore, ExecutionStore executionStore, Clock clock, JobKey jobKey, Object payload) {
        this.jobStore = jobStore;
        this.executionStore = executionStore;
        this.clock = clock;
        this.jobKey = jobKey;
        this.payload = payload;
    }

    @Override
    public ScheduleCommand priority(Priority priority) {
        this.priority = Objects.requireNonNull(priority, "priority");
        return this;
    }

    @Override
    public ScheduleCommand as(String actor) {
        Objects.requireNonNull(actor, "actor");
        if (actor.isBlank()) {
            throw new IllegalArgumentException("actor must not be blank");
        }
        // o actor do scheduler é load-bearing (rearme fixed-delay, cura do upsert —
        // ADR-0035): forjá-lo faria um agendamento manual dirigir a corrente do trigger.
        // Case/espaço-insensível porque o predicado da cura roda no banco, e a collation
        // default de MySQL/SQL Server é case-insensitive — os dois avaliadores do mesmo
        // predicado precisam de uma única semântica, normalizada na borda de entrada
        if (Execution.SCHEDULER_ACTOR.equalsIgnoreCase(actor.strip())) {
            throw new IllegalArgumentException("actor '" + Execution.SCHEDULER_ACTOR
                    + "' is reserved for engine-fired occurrences (ADR-0035) — identify the real caller");
        }
        this.actor = actor;
        return this;
    }

    @Override
    public ScheduleCommand idempotencyKey(String key) {
        this.idempotencyKey = Objects.requireNonNull(key, "key");
        return this;
    }

    @Override
    public Enqueued now() {
        return at(clock.instant());
    }

    @Override
    public Enqueued at(Instant when) {
        Objects.requireNonNull(when, "when");
        // job precisa existir antes do disparo, não só no boot — mohs_executions.job_key
        // tem FK pra mohs_job_definitions; sem esta checagem, o erro que o chamador veria
        // seria uma violação de FK crua, não uma mensagem que ensina.
        jobStore.find(jobKey).orElseThrow(() -> new IllegalArgumentException(
                "no job registered for id '" + jobKey.value() + "' — call Mohs.define first"));

        ExecutionId id = ExecutionId.of(UUIDv7.randomUUIDString());
        Execution execution = new Execution(id, jobKey, ExecutionState.ENQUEUED, when, null, List.of(), actor, priority, idempotencyKey);
        try {
            executionStore.insert(execution, payload);
            return new Enqueued(id, jobKey, when, actor);
        } catch (DuplicateKeyException e) {
            if (idempotencyKey == null) {
                throw e;
            }
            // Idempotent Receiver (EIP): o índice único uq_mohs_executions_idem
            // resolveu a corrida — devolve o recibo da execução original, mesma
            // resposta pro retry do cliente, zero duplicação. Corrida decidida
            // pelo banco, nunca por SELECT prévio (mesmo espírito do CONC-2).
            Execution existing = executionStore.findByIdempotencyKey(jobKey, idempotencyKey)
                    .orElseThrow(() -> e);
            return new Enqueued(existing.id(), existing.jobKey(), existing.scheduledAt(), existing.actor());
        }
    }

    @Override
    public Enqueued after(Duration delay) {
        Objects.requireNonNull(delay, "delay");
        return at(clock.instant().plus(delay));
    }
}
