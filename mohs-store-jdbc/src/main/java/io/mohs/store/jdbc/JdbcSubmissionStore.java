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
package io.mohs.store.jdbc;

import java.util.Objects;
import java.util.List;
import java.time.Duration;
import java.time.Instant;
import org.jspecify.annotations.Nullable;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import io.mohs.core.execution.Execution;
import io.mohs.core.job.JobKey;
import io.mohs.engine.HistoryStore;
import io.mohs.engine.SubmissionStore.NewExecution;
import io.mohs.engine.Shards;
import io.mohs.engine.TriggerFirer;
import io.mohs.engine.WorkQueue;
import io.mohs.store.jdbc.delegate.JdbcDelegate;

import javax.sql.DataSource;

import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;

import io.mohs.engine.SubmissionStore;

/** Atomic creation of executions, shared by application submission and recurring firing. */
public final class JdbcSubmissionStore implements SubmissionStore, TriggerFirer {
    private final TransactionTemplate transactionTemplate;
    private final TransactionTemplate firingTransaction;
    private final NamedParameterJdbcTemplate jdbc;
    private final JdbcDelegate dialect;
    private final Duration idempotencyRetention;

    /**
     * @param dataSource the single database used by every write in this operation
     * @param dialect the database-specific statements and JDBC types
     * @param idempotencyRetention the receipt lifetime; zero retains receipts indefinitely
     */
    public JdbcSubmissionStore(DataSource dataSource, JdbcDelegate dialect, Duration idempotencyRetention) {
        Objects.requireNonNull(dataSource, "dataSource");
        this.dialect = Objects.requireNonNull(dialect, "dialect");
        this.idempotencyRetention = Objects.requireNonNull(idempotencyRetention, "idempotencyRetention");
        if (idempotencyRetention.isNegative()) throw new IllegalArgumentException("idempotencyRetention must be nonnegative");
        io.mohs.core.TimePrecision.micros(idempotencyRetention);
        jdbc = JdbcSupport.tickTemplate(dataSource);
        var manager = new DataSourceTransactionManager(dataSource);
        transactionTemplate = new TransactionTemplate(manager);
        transactionTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_NESTED);
        transactionTemplate.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
        transactionTemplate.setTimeout(3);
        firingTransaction = new TransactionTemplate(manager, transactionTemplate);
        firingTransaction.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
    }

    /**
     * {@code status.isNewTransaction()} is the one honest signal of "durable now" versus "durable
     * with the host": the thread-level "is a transaction active" would also say yes for a host
     * transaction on ANOTHER DataSource, inside which this template opens and commits a transaction
     * of its own — and an event tied to that host's commit would then trail the execution, or be
     * lost to a rollback that never touched it.
     */
    @Override
    public void accept(List<NewExecution> executions, @Nullable NewBatch batch,
            Runnable onDurable) {
        var prepared = java.util.List.copyOf(executions);
        if (prepared.isEmpty()) throw new IllegalArgumentException("submission requires at least one execution");
        if (prepared.stream().map(p -> p.executionId()).distinct().count() != prepared.size()) {
            throw new IllegalArgumentException("duplicate execution identity in submission");
        }
        var contracts = new java.util.TreeMap<String, String>();
        for (var execution : prepared) {
            String previous = contracts.putIfAbsent(execution.jobKey().value(), execution.payload().inputContract());
            if (previous != null && !previous.equals(execution.payload().inputContract())) {
                throw new IllegalArgumentException("conflicting input contracts for one job in submission");
            }
            if (execution.origin() != SubmissionStore.Origin.MANUAL
                    || !Objects.equals(execution.batchId(), batch == null ? null : batch.id())
                    || batch != null && execution.idempotencyKey() != null) {
                throw new IllegalArgumentException("manual submission origin or batch membership is inconsistent");
            }
        }
        Objects.requireNonNull(onDurable, "onDurable");
        boolean joinedHost = Boolean.TRUE.equals(transactionTemplate.execute(status -> {
            // Checked BEFORE the writes: a host transaction without synchronization (a manager set to
            // SYNCHRONIZATION_NEVER, a connection bound by hand) cannot carry the after-commit hook,
            // and finding that out after the enqueue would be a rollback with Spring's generic message
            if (!status.isNewTransaction() && !TransactionSynchronizationManager.isSynchronizationActive()) {
                throw new IllegalStateException("the enqueue joined a host transaction that has no transaction"
                        + " synchronization, so Mohs cannot publish Enqueued after its commit — keep synchronization"
                        + " on the host's PlatformTransactionManager (the default) or schedule outside the transaction");
            }
            // Consume the locking read itself: under host REPEATABLE_READ, an ordinary follow-up
            // SELECT could see a stale pre-retirement snapshot on MySQL.
            for (var contractEntry : contracts.entrySet()) {
                String key = contractEntry.getKey();
                var active = jdbc.query(dialect.lockBindingOwner(),
                        new MapSqlParameterSource("jobKey", key),
                        (rs, _) -> rs.getBoolean("retired")
                                ? null : rs.getString("input_contract"));
                if (active.isEmpty() || active.getFirst() == null) {
                    throw new IllegalArgumentException("no active job '" + key + "' at acceptance");
                }
                String contract = active.getFirst();
                if (!contract.equals(contractEntry.getValue())) {
                    throw new IllegalArgumentException("input contract differs from the accepted definition for '" + key + "'");
                }
            }
            persist(prepared, batch);
            if (status.isNewTransaction()) {
                return false;
            }
            JdbcDurableCallbacks.register(this, onDurable);
            return true;
        }));
        if (!joinedHost) {
            onDurable.run();
        }
    }

    @Override
    public boolean fire(JobKey key, long observedRevision, Instant observedNextFireAt, @Nullable Instant newNextFireAt,
            List<Execution> occurrences, Instant now) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(observedNextFireAt, "observedNextFireAt");
        var prepared = List.copyOf(occurrences);
        if (observedRevision < 1 || prepared.stream().anyMatch(e -> !e.jobKey().equals(key)
                || e.batchId() != null || e.idempotencyKey() != null)
                || prepared.stream().map(Execution::id).distinct().count() != prepared.size()) {
            throw new IllegalArgumentException("occurrences must have distinct identities for this trigger");
        }
        Objects.requireNonNull(now, "now");
        return Boolean.TRUE.equals(firingTransaction.execute(_ -> {
            Instant databaseNow = JdbcDatabaseTime.now(jdbc, dialect);
            if (observedNextFireAt.isAfter(now) || now.isAfter(databaseNow)) return false;
            if (prepared.stream().anyMatch(e -> e.scheduledAt().isAfter(now))) {
                throw new IllegalArgumentException("a firing cannot materialize occurrences beyond its observed cutoff");
            }
            int advanced = jdbc.update(dialect.advanceTriggerByCas(),
                    new MapSqlParameterSource()
                            .addValue("jobKey", key.value())
                            .addValue("observedNextFireAt", JdbcTimestamps.toUtcLocalDateTime(observedNextFireAt))
                            .addValue("newNextFireAt", JdbcTimestamps.toUtcLocalDateTimeOrNull(newNextFireAt == null
                                    ? null : io.mohs.core.TimePrecision.eligible(newNextFireAt)))
                            .addValue("retired", false).addValue("paused", false)
                            .addValue("observedRevision", observedRevision));
            if (advanced == 0) {
                return false;
            }
            // The definition lock is also held by bound upserts: input-only updates cannot
            // race a stale snapshot into history after the update has committed.
            var storedInput = jdbc.queryForObject(dialect.findJobBinding(),
                    new MapSqlParameterSource("jobKey", key.value()), (rs, _) ->
                        new io.mohs.engine.JobPayload(rs.getString("recurring_payload") == null
                                ? "{}" : rs.getString("recurring_payload"), rs.getString("input_contract")));
            // The occurrence retains its scheduled instant; createdAt records this firing decision.
            var executions = prepared.stream()
                    .map(occurrence -> new SubmissionStore.NewExecution(occurrence.id(), occurrence.jobKey(),
                            Shards.of(occurrence.id()), occurrence.priority().value(), occurrence.scheduledAt(), now,
                            occurrence.actor(), occurrence.batchId(), occurrence.idempotencyKey(), storedInput, SubmissionStore.Origin.SCHEDULED, observedRevision))
                    .toList();
            persist(executions, null);
            return true;
        }));
    }
    private void persist(List<NewExecution> executions, @Nullable NewBatch batch) {
        if (executions.isEmpty()) return;
        Instant acceptedAt = batch != null || executions.stream().anyMatch(e -> e.idempotencyKey() != null)
                ? JdbcDatabaseTime.now(jdbc, dialect) : executions.getFirst().createdAt();
        if (batch != null) {
            jdbc.update(dialect.insertBatch(), new MapSqlParameterSource("id", batch.id())
                    .addValue("name", batch.name()).addValue("total", (long) executions.size())
                    .addValue("createdAt", dialect.splitTimestamp(acceptedAt)));
        }
        // Idempotency goes in BEFORE history: the primary-key conflict IS the deduplication check
        // (Idempotent Receiver, EIP) and has to abort the whole unit before any history row is born
        MapSqlParameterSource[] keys = executions.stream()
                .filter(e -> e.idempotencyKey() != null)
                .sorted(java.util.Comparator.comparing((NewExecution e) -> e.jobKey().value()).thenComparing(NewExecution::idempotencyKey))
                .map(e -> idempotencyParams(e, acceptedAt))
                .toArray(MapSqlParameterSource[]::new);
        if (keys.length > 0) {
            jdbc.batchUpdate("DELETE FROM mohs_idempotency WHERE job_key = :jobKey AND idempotency_key = :idempotencyKey AND expires_at <= :createdAt", keys);
            jdbc.batchUpdate(dialect.insertIdempotency(), keys);
        }
        jdbc.batchUpdate(dialect.recordExecution(), executions.stream()
                .map(this::recordParams)
                .toArray(MapSqlParameterSource[]::new));
        jdbc.batchUpdate(dialect.readyInsert(), executions.stream().map(e ->
                JdbcSupport.readyEntryParams(new WorkQueue.ReadyEntry(e.executionId(), e.jobKey(), e.shard(),
                        e.priority(), 1, e.scheduledAt()), dialect)).toArray(MapSqlParameterSource[]::new));
    }

    private MapSqlParameterSource idempotencyParams(NewExecution execution, Instant acceptedAt) {
        return new MapSqlParameterSource()
                .addValue("jobKey", execution.jobKey().value())
                .addValue("idempotencyKey", execution.idempotencyKey())
                .addValue("executionId", execution.executionId().value())
                .addValue("scheduledAt", dialect.splitTimestamp(execution.scheduledAt()))
                .addValue("actor", execution.actor())
                .addValue("expiresAt", idempotencyRetention.isZero() ? null : dialect.splitTimestamp(io.mohs.core.TimePrecision.eligible(acceptedAt.plus(idempotencyRetention))), java.sql.Types.TIMESTAMP)
                .addValue("createdAt", dialect.splitTimestamp(acceptedAt));
    }

    private MapSqlParameterSource recordParams(NewExecution execution) {
        return new MapSqlParameterSource()
                .addValue("executionId", execution.executionId().value())
                .addValue("jobKey", execution.jobKey().value())
                .addValue("shard", execution.shard())
                .addValue("priority", execution.priority())
                .addValue("scheduledAt", dialect.splitTimestamp(execution.scheduledAt()))
                .addValue("createdAt", dialect.splitTimestamp(execution.createdAt()))
                .addValue("actor", execution.actor())
                .addValue("batchId", execution.batchId())
                .addValue("origin", execution.origin().name())
                .addValue("scheduleRevision", execution.scheduleRevision())
                .addValue("idempotencyKey", execution.idempotencyKey())
                .addValue("payload", execution.payload().json())
                .addValue("inputContract", execution.payload().inputContract());
    }

}
