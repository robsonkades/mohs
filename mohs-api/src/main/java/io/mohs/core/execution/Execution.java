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
package io.mohs.core.execution;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

import io.mohs.core.job.JobKey;
import io.mohs.core.definition.JobDefinition;

import org.jspecify.annotations.Nullable;

/**
 * A single firing of a {@link JobDefinition}: its identity, current state, the actor that caused
 * the firing, and every {@link Attempt} made so far. The actor trail is non-negotiable on every
 * invocation.
 *
 * <p>{@code firedAt} is the instant the execution was claimed by a node, and is {@code null} until
 * that happens (in {@link ExecutionState#ENQUEUED}, for instance). Since ownership moved into
 * {@code mohs_lease} it also returns to {@code null} AFTER completion — the historical question
 * "when did each attempt start" belongs to {@link Attempt#startedAt}.
 *
 * <p>{@code owner} answers "WHO is running this right now": the {@code node_id} holding ownership
 * while {@link ExecutionState#RUNNING}, and {@code null} otherwise.
 *
 * @param id the identity of the execution
 * @param jobKey the stable identity of the job
 * @param state the current derived execution state
 * @param scheduledAt the intended firing instant
 * @param firedAt the instant this attempt began
 * @param attempts the recorded attempts of this execution
 * @param actor the identity attributed to the operation
 * @param priority the ordering priority used when claiming work
 * @param idempotencyKey the optional key used to deduplicate scheduling requests
 * @param batchId the identity of the batch
 * @param owner the node currently owning the execution
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
     * The actor of occurrences materialised by a recurring trigger — how a firing by the engine
     * itself is told apart from a manual or programmatic schedule.
     *
     * <p>The name is <b>reserved</b>: beyond the audit trail, it carries an engine decision (a
     * fixed-delay chain is only rearmed on a scheduler occurrence), so {@code ScheduleCommand.as}
     * and the REST API both reject it — a manual schedule may never pass itself off as the engine.
     */
    public static final String SCHEDULER_ACTOR = "scheduler";

    /**
     * Creates a {@code Execution} with the supplied values.
     *
     * @param id the identity of the execution
     * @param jobKey the stable identity of the job
     * @param state the current derived execution state
     * @param scheduledAt the intended firing instant
     * @param firedAt the instant this attempt began
     * @param attempts the recorded attempts of this execution
     * @param actor the identity attributed to the operation
     * @param priority the ordering priority used when claiming work
     * @param idempotencyKey the optional key used to deduplicate scheduling requests
     * @param batchId the identity of the batch
     * @param owner the node currently owning the execution
     */
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
        attempts = List.copyOf(attempts); // a defensive copy (Effective Java, Item 50)
    }

    /**
     * {@link Priority#NORMAL}, no {@code idempotencyKey}, outside any batch and with no current owner — the same defaults as the schema (`DEFAULT 20`).
     *
     * @param id the identity of the execution
     * @param jobKey the stable identity of the job
     * @param state the current derived execution state
     * @param scheduledAt the intended firing instant
     * @param firedAt the instant this attempt began
     * @param attempts the recorded attempts of this execution
     * @param actor the identity attributed to the operation
     */
    public Execution(ExecutionId id, JobKey jobKey, ExecutionState state, Instant scheduledAt,
            @Nullable Instant firedAt, List<Attempt> attempts, String actor) {
        this(id, jobKey, state, scheduledAt, firedAt, attempts, actor, Priority.NORMAL, null, null, null);
    }

    /**
     * A freshly enqueued occurrence: no attempts, no firing, no {@code idempotencyKey}, outside any
     * batch and with no current owner.
     *
     * <p>A NAMED factory replacing the two overloads with an AMBIGUOUS tail that used to live here
     * (the seven-argument one, which ends in {@code actor}, remains — nothing in it can be
     * transposed). Effective Java, Item 1: the removed overloads ended in a row of
     * {@code @Nullable String}, and {@code (priority, null, nodeId)} compiled while writing the
     * {@code nodeId} into {@code batchId} — an execution declaring itself a member of a batch that
     * does not exist, with nothing to flag it. This type is a READ model: the user never constructs
     * it, and internal convenience did not justify publishing the trap.
     *
     * @param id the identity of the execution
     * @param jobKey the stable identity of the job
     * @param scheduledAt the intended firing instant
     * @param actor the identity attributed to the operation
     * @param priority the ordering priority used when claiming work
     * @return the new execution with no attempts or owner
     */
    public static Execution enqueued(ExecutionId id, JobKey jobKey, Instant scheduledAt, String actor,
            Priority priority) {
        return new Execution(id, jobKey, ExecutionState.ENQUEUED, scheduledAt, null, List.of(), actor, priority,
                null, null, null);
    }
}
