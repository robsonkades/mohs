package io.mohs.engine;

import java.util.List;
import java.time.Instant;
import io.mohs.core.execution.ExecutionId;
import io.mohs.core.job.JobKey;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/** Atomic acceptance of manual work. Job retirement is arbitrated inside this operation.
 * A host transaction on the same data source owns durability; an exception after commit may
 * leave the outcome unknown. An idempotency key allows callers to read the winning receipt.
 */
public interface SubmissionStore {
    /** Explicit provenance; actor is only an audit identity. */
    enum Origin { MANUAL, SCHEDULED }

    /**
     * An accepted execution, ready for its birth record.
     *
     * <p>{@code createdAt} is the enqueue instant — a column of the row, no longer part of its
     * identity: the primary key is {@code execution_id} on every database, so nothing about it needs
     * to travel to the completion. {@code batchId} identifies optional group membership.
     *
     * @param executionId the identity of the execution
     * @param jobKey the stable identity of the job
     * @param shard the deterministic queue partition
     * @param priority the ordering priority used when claiming work
     * @param scheduledAt the intended firing instant
     * @param createdAt the enqueue instant
     * @param actor the identity attributed to the operation
     * @param batchId the batch identity, or {@code null} for an independent execution
     * @param idempotencyKey the optional key used to deduplicate scheduling requests
     * @param payload the input passed to the job handler
     * @param origin whether a trigger or an application created this execution
     * @param scheduleRevision the accepted trigger revision, present only for scheduled occurrences
     */
    record NewExecution(
            ExecutionId executionId,
            JobKey jobKey,
            int shard,
            int priority,
            Instant scheduledAt,
            Instant createdAt,
            String actor,
            @Nullable String batchId,
            @Nullable String idempotencyKey,
            JobPayload payload,
            Origin origin,
            @Nullable Long scheduleRevision
    ) {
        /**
         * Creates a {@code NewExecution} with the supplied values.
         *
         * @param executionId the identity of the execution
         * @param jobKey the stable identity of the job
         * @param shard the deterministic queue partition
         * @param priority the ordering priority used when claiming work
         * @param scheduledAt the intended firing instant
         * @param createdAt the enqueue instant
         * @param actor the identity attributed to the operation
         * @param batchId the batch identity, or {@code null} for an independent execution
         * @param idempotencyKey the optional key used to deduplicate scheduling requests
         * @param payload the input passed to the job handler
     * @param origin whether a trigger or an application created this execution
     * @param scheduleRevision the accepted trigger revision, present only for scheduled occurrences
         */
        public NewExecution {
            Objects.requireNonNull(executionId, "executionId");
            if (idempotencyKey != null) io.mohs.core.TechnicalKey.require(idempotencyKey, "idempotencyKey");
            Objects.requireNonNull(jobKey, "jobKey");
            if (shard < 0 || shard >= Shards.SHARD_COUNT) {
                throw new IllegalArgumentException("shard must be in [0, " + Shards.SHARD_COUNT + ")");
            }
            io.mohs.core.execution.Priority.fromValue(priority);
            Objects.requireNonNull(scheduledAt, "scheduledAt");
            Objects.requireNonNull(createdAt, "createdAt");
            scheduledAt = io.mohs.core.TimePrecision.eligible(scheduledAt);
            createdAt = io.mohs.core.TimePrecision.observed(createdAt);
            Objects.requireNonNull(actor, "actor");
            if (actor.isBlank() || actor.length() > 255) {
                throw new IllegalArgumentException("actor must be nonblank and at most 255 characters");
            }
            if (batchId != null) io.mohs.core.GeneratedId.require(batchId, "batchId");
            Objects.requireNonNull(payload, "payload");
            Objects.requireNonNull(origin, "origin");
            if (origin == Origin.MANUAL ? scheduleRevision != null : scheduleRevision == null || scheduleRevision < 1) {
                throw new IllegalArgumentException("scheduled executions require a positive revision; manual executions have none");
            }
        }
    }


    /** The total is derived from the supplied members, never independently supplied. */
    record NewBatch(String id, String name) {
        public NewBatch {
            io.mohs.core.GeneratedId.require(id, "batchId");
            Objects.requireNonNull(name, "name");
            if (name.isBlank() || name.length() > 255) {
                throw new IllegalArgumentException("batch name must be nonblank and at most 255 characters");
            }
        }
    }

    /** Accepts all records, receipts and ready work together. Runs onDurable only after commit. */
    void accept(List<SubmissionStore.NewExecution> executions, @Nullable NewBatch batch, Runnable onDurable);
}
