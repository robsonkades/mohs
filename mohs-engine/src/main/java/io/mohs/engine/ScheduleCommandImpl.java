/*
 * Copyright 2026 The Mohs Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
 * {@link ScheduleCommand} over the persistence ports — it accumulates
 * {@code priority}/{@code actor}/{@code idempotencyKey} until a terminal
 * ({@code now}/{@code at}/{@code after}) writes the execution; a chain abandoned before the terminal
 * never touches the database.
 *
 * <p>The terminal is the enqueue unit — history plus queue (plus idempotency) in a SINGLE
 * transaction through {@link StoreTransactions}, which joins the host's transaction when there is
 * one. That is what makes the orphan key and the queueless execution that autocommit mode would
 * allow structurally impossible.
 */
final class ScheduleCommandImpl implements ScheduleCommand {

    private final JobStore jobStore;
    private final HistoryStore historyStore;
    private final WorkQueue workQueue;
    private final StoreTransactions storeTransactions;
    private final Clock clock;
    private final JobKey jobKey;
    private final Object payload;
    private final Runnable localWakeSignal;

    private Priority priority = Priority.NORMAL;
    private String actor = MohsImpl.DEFAULT_ACTOR;
    private @Nullable String idempotencyKey;

    ScheduleCommandImpl(JobStore jobStore, HistoryStore historyStore, WorkQueue workQueue,
            StoreTransactions storeTransactions, Clock clock, JobKey jobKey, Object payload,
            Runnable localWakeSignal) {
        this.jobStore = jobStore;
        this.historyStore = historyStore;
        this.workQueue = workQueue;
        this.storeTransactions = storeTransactions;
        this.clock = clock;
        this.jobKey = jobKey;
        this.payload = payload;
        this.localWakeSignal = localWakeSignal;
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
        // The scheduler actor is load-bearing (the fixed-delay rearm, the upsert's cure): forging it
        // would let a manual schedule drive the trigger's chain. Case- and whitespace-insensitive
        // because the cure's predicate runs in the database, and MySQL's and SQL Server's default
        // collation is case-insensitive — both evaluators of the same predicate need one semantics,
        // normalised at the entry boundary
        if (Execution.SCHEDULER_ACTOR.equalsIgnoreCase(actor.strip())) {
            throw new IllegalArgumentException("actor '" + Execution.SCHEDULER_ACTOR
                    + "' is reserved for engine-fired occurrences — identify the real caller");
        }
        this.actor = actor;
        return this;
    }

    @Override
    public ScheduleCommand idempotencyKey(String key) {
        Objects.requireNonNull(key, "key");
        // A blank key is never an intent to deduplicate — accepted, every keyless-by-mistake schedule
        // of the job would collapse into the first one, silently
        if (key.isBlank()) {
            throw new IllegalArgumentException("idempotency key must not be blank");
        }
        if (key.length() > MAX_IDEMPOTENCY_KEY_LENGTH) {
            throw new IllegalArgumentException("idempotency key must be at most " + MAX_IDEMPOTENCY_KEY_LENGTH
                    + " characters, got " + key.length());
        }
        this.idempotencyKey = key;
        return this;
    }

    @Override
    public Enqueued now() {
        return at(clock.instant());
    }

    @Override
    public Enqueued at(Instant when) {
        Objects.requireNonNull(when, "when");
        // The job has to exist at firing time, not only at boot — without this check the caller would
        // see a raw error rather than a message that teaches.
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
            // Already due, so wake the local loop; a future one is left to the poll — waking now would
            // be a lap that still does not see it
            if (!when.isAfter(clock.instant())) {
                localWakeSignal.run();
            }
            return new Enqueued(id, jobKey, when, actor);
        } catch (DuplicateKeyException e) {
            if (idempotencyKey == null) {
                throw e;
            }
            // Idempotent Receiver (EIP): mohs_idempotency's primary-key conflict resolved the race —
            // return the original execution's receipt, the same answer for the client's retry, with
            // zero duplication. The race is decided by the database, never by a prior SELECT.
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
