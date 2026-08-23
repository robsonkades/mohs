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
import io.mohs.core.execution.Priority;
import io.mohs.core.job.JobKey;

/**
 * {@link ScheduleCommand} sobre as portas da Phase 5 — acumula
 * {@code priority}/{@code actor}/{@code idempotencyKey} até um terminal
 * ({@code now}/{@code at}/{@code after}) gravar a execução; uma cadeia
 * abandonada antes do terminal nunca toca o banco.
 *
 * <p>O terminal é a unidade de enqueue do §7.5-1 — história + fila (+
 * idempotência) numa ÚNICA transação via {@link StoreTransactions}, que se
 * junta à transação do host quando existe (ADR-0003 §4). É o que torna
 * estruturalmente impossível a chave órfã e a execução sem fila que o
 * modo autocommit permitiria (review S5.2).
 */
final class ScheduleCommandImpl implements ScheduleCommand {

    private final JobStore jobStore;
    private final HistoryStore historyStore;
    private final WorkQueue workQueue;
    private final StoreTransactions storeTransactions;
    private final Clock clock;
    private final JobKey jobKey;
    private final Object payload;

    private Priority priority = Priority.NORMAL;
    private String actor = MohsImpl.DEFAULT_ACTOR;
    private @Nullable String idempotencyKey;

    ScheduleCommandImpl(JobStore jobStore, HistoryStore historyStore, WorkQueue workQueue,
            StoreTransactions storeTransactions, Clock clock, JobKey jobKey, Object payload) {
        this.jobStore = jobStore;
        this.historyStore = historyStore;
        this.workQueue = workQueue;
        this.storeTransactions = storeTransactions;
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
        // job precisa existir antes do disparo, não só no boot — sem esta
        // checagem, o erro que o chamador veria seria cru, não uma mensagem
        // que ensina.
        jobStore.find(jobKey).orElseThrow(() -> new IllegalArgumentException(
                "no job registered for id '" + jobKey.value() + "' — call Mohs.define first"));

        ExecutionId id = ExecutionId.of(UUIDv7.randomUUIDString());
        Instant createdAt = clock.instant();
        try {
            storeTransactions.inTransaction(() -> {
                int shard = Shards.of(id);
                historyStore.record(List.of(new HistoryStore.NewExecution(id, jobKey, shard, priority.value(),
                        when, createdAt, actor, null, idempotencyKey, payload)));
                workQueue.offer(List.of(new WorkQueue.ReadyEntry(id, jobKey, shard, priority.value(), 1, when)));
            });
            return new Enqueued(id, jobKey, when, actor);
        } catch (DuplicateKeyException e) {
            if (idempotencyKey == null) {
                throw e;
            }
            // Idempotent Receiver (EIP): o conflito de PK de mohs_idempotency
            // resolveu a corrida — devolve o recibo da execução original, mesma
            // resposta pro retry do cliente, zero duplicação. Corrida decidida
            // pelo banco, nunca por SELECT prévio (mesmo espírito do CONC-2).
            ExecutionId winner = historyStore.findByIdempotencyKey(jobKey, idempotencyKey).orElseThrow(() -> e);
            Execution existing = historyStore.find(winner, clock.instant()).orElseThrow(() -> e);
            return new Enqueued(existing.id(), existing.jobKey(), existing.scheduledAt(), existing.actor());
        }
    }

    @Override
    public Enqueued after(Duration delay) {
        Objects.requireNonNull(delay, "delay");
        return at(clock.instant().plus(delay));
    }
}
