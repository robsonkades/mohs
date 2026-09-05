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

import java.time.Instant;
import java.util.List;
import java.util.Objects;

import javax.sql.DataSource;

import org.jspecify.annotations.Nullable;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import io.mohs.core.execution.Execution;
import io.mohs.core.job.JobKey;
import io.mohs.engine.HistoryStore;
import io.mohs.engine.Shards;
import io.mohs.engine.TriggerFirer;
import io.mohs.engine.WorkQueue;
import io.mohs.store.jdbc.delegate.JdbcDelegate;

/**
 * {@link TriggerFirer} over {@code mohs_job_definitions} plus the split tables: the trigger's advance
 * CAS and the occurrences' birth — history ({@code record}) and queue ({@code offer}) — in a single
 * transaction, exactly the enqueue unit with the CAS as a cluster-wide mutual-exclusion guard.
 *
 * <p>{@code historyStore}/{@code workQueue} must point at the same {@code DataSource} passed here — that
 * is how they take part in the transaction.
 *
 * <p>The CAS compares {@code next_fire_at} against the value {@code findDueRecurring} READ from the
 * column itself — never an instant computed in the JVM that never went through the database (temporal
 * precision does not make a guaranteed round trip between the JVM and the four databases; a value read
 * and re-serialised by {@link JdbcTimestamps} compares equal by construction).
 */
public final class JdbcTriggerFirer implements TriggerFirer {

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final TransactionTemplate transactionTemplate;
    private final HistoryStore historyStore;
    private final WorkQueue workQueue;
    private final JdbcDelegate delegate;

    /**
     * Creates a {@code JdbcTriggerFirer} with the supplied values.
     *
     * @param dataSource the configured database connection source
     * @param historyStore the persistence port for execution history
     * @param workQueue the persistence port for ready work
     * @param delegate the database-specific SQL and timestamp adapter
     */
    public JdbcTriggerFirer(DataSource dataSource, HistoryStore historyStore, WorkQueue workQueue,
            JdbcDelegate delegate) {
        Objects.requireNonNull(dataSource, "dataSource");
        // The firing CAS runs on the engine's loop thread; the history record and the queue offer it
        // is followed by keep their owners' templates
        this.jdbcTemplate = JdbcSupport.tickTemplate(dataSource);
        this.transactionTemplate = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
        // The same reasoning as in JdbcWorkQueue: a guarded CAS assumes "last write wins"
        // (READ COMMITTED), and does not inherit the database's default.
        this.transactionTemplate.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
        this.historyStore = Objects.requireNonNull(historyStore, "historyStore");
        this.workQueue = Objects.requireNonNull(workQueue, "workQueue");
        this.delegate = Objects.requireNonNull(delegate, "delegate");
    }

    @Override
    public boolean fire(JobKey key, Instant observedNextFireAt, @Nullable Instant newNextFireAt,
            List<Execution> occurrences, Object payload, Instant now) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(observedNextFireAt, "observedNextFireAt");
        Objects.requireNonNull(occurrences, "occurrences");
        Objects.requireNonNull(payload, "payload");
        Objects.requireNonNull(now, "now");
        return Boolean.TRUE.equals(transactionTemplate.execute(_ -> {
            int advanced = jdbcTemplate.update(delegate.advanceTriggerByCas(),
                    new MapSqlParameterSource()
                            .addValue("jobKey", key.value())
                            .addValue("observedNextFireAt", JdbcTimestamps.toUtcLocalDateTime(observedNextFireAt))
                            .addValue("newNextFireAt", JdbcTimestamps.toUtcLocalDateTimeOrNull(newNextFireAt))
                            .addValue("retired", false));
            if (advanced == 0) {
                return false;
            }
            // createdAt = now, not scheduledAt: it is the instant the row is BORN, and in a FIRE_ALL
            // misfire the scheduledAt is in the past — history would record a birth that did not happen
            // then. It leads mohs_execution's primary key and travels in memory until the completion,
            // which matches the row by equality. (The reason used to be different: it was the partition
            // key, and an old scheduledAt would point at a partition retention might already have
            // dropped.)
            // visible_at = scheduledAt: the occurrence enters the queue already due
            historyStore.record(occurrences.stream()
                    .map(occurrence -> new HistoryStore.NewExecution(occurrence.id(), occurrence.jobKey(),
                            Shards.of(occurrence.id()), occurrence.priority().value(), occurrence.scheduledAt(), now,
                            occurrence.actor(), occurrence.batchId(), occurrence.idempotencyKey(), payload))
                    .toList());
            workQueue.offer(occurrences.stream()
                    .map(occurrence -> new WorkQueue.ReadyEntry(occurrence.id(), occurrence.jobKey(),
                            Shards.of(occurrence.id()), occurrence.priority().value(), 1, occurrence.scheduledAt()))
                    .toList());
            return true;
        }));
    }
}
