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
package io.mohs.store.jdbc.dialect;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.List;

import org.jspecify.annotations.Nullable;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import io.mohs.store.jdbc.JdbcTimestamps;

/**
 * The few real dialect divergences {@code io.mohs.store.jdbc} needs — modelled on Hibernate's
 * {@code LimitHandler}/{@code LockingStrategy} shape (small interfaces, one concern each), without
 * taking Hibernate as a dependency: those two interfaces live inside {@code Dialect}, which only exists
 * after initialising a {@code SessionFactory}/{@code ServiceRegistry} — using them in isolation would
 * mean adopting much of the framework anyway.
 *
 * <p>Each implementation owns the claim's entire SQL template
 * ({@link #claimReady}/{@link #selectReadyCandidates}), not concatenable fragments — SQL Server's
 * {@code TOP} changes <b>position</b> in the query (right after {@code SELECT}, not at the end like
 * {@code LIMIT}), so a composition of generic fragments does not close cleanly. The same pattern Quartz
 * itself uses ({@code StdJDBCDelegate}/{@code MSSQLDelegate}: each Delegate holds the complete SQL for
 * each operation) and how Hibernate actually implements {@code LimitHandler} underneath (it receives
 * the SQL and returns the rewritten SQL, not a fragment).
 *
 * <p>An explicit choice, never auto-detection — the same pattern as Quartz's
 * ({@code org.quartz.jobStore.driverDelegateClass}): detecting through
 * {@code Connection.getMetaData()} is fragile across driver forks and versions.
 */
public interface JdbcDialect {

    /**
     * Where this dialect's Flyway migrations live — a classpath location outside the default
     * {@code db/migration} on purpose, so it is never swept by the host's Flyway. Each dialect knows its
     * own folder for the same reason it knows its own SQL.
     */
    String migrationLocation();

    /**
     * A portable row ceiling ({@code io.mohs.store.jdbc.JdbcHistoryStore#findPage},
     * {@code JdbcLeaseStore#findOrphaned}): {@code :limit} is the same named parameter in both methods;
     * only the text's position in the query changes. The default covers H2, Postgres and MySQL
     * ({@code LIMIT} at the end); SQL Server overrides both together ({@code TOP} right after
     * {@code SELECT}).
     */
    default String topClause() {
        return "";
    }

    default String limitClause() {
        return "LIMIT :limit";
    }

    /**
     * A table hint for reads that must not take locks: the principle is that monitoring and probing must
     * never contend with the claim/completion hot path.
     *
     * <p>The default is empty — on H2, Postgres and MySQL an MVCC {@code SELECT} already takes no row
     * locks (a consistent read). SQL Server overrides it: under the default {@code READ COMMITTED}
     * (without RCSI) every {@code SELECT} takes shared locks that block, and are blocked by, the
     * engine's {@code UPDATE}s. The accepted anomalies (a row in transition, double counting or loss
     * under a page split, error 601 as a transient failure) are documented on SQL Server's override;
     * never use it on a read that hydrates an entity.
     *
     * <p>Today the ONLY caller is the idle gate's probe ({@code JdbcWorkQueue#hasVisibleWork}) — hence
     * "Read" rather than "Count" in the name. It is even more tolerant than a count: losing a row under
     * a page split only becomes a false negative if the scan loses ALL the visible ones, and the outcome
     * is one poll; error 601 falls into the {@code Engine}'s fail-open fallback, which returns the tick
     * to the lap.
     *
     * <p>{@code GET /overview}'s counts ({@code JdbcHistoryStore#countActiveByState} and
     * {@code countTerminalOutcomesSince}) were rewritten over the split tables and NO longer use the
     * hint — on SQL Server without RCSI they take shared locks on all three hot tables again. Tracked in
     * tracked separately; it is not a regression of this method.
     */
    default String lockFreeReadHint() {
        return "";
    }

    /**
     * The temporal crossing for the split tables: they were born {@code TIMESTAMPTZ} on Postgres and
     * remain zoneless in the functional equivalents.
     *
     * <p>The default is the usual UTC {@code LocalDateTime} crossing; Postgres overrides it with
     * {@code OffsetDateTime} — a {@code LocalDateTime} against a tz-aware column would be interpreted in
     * the SESSION's zone, the class of bug that crossing was introduced to kill. The control-plane
     * tables ({@code mohs_job_definitions}/{@code mohs_nodes}/{@code mohs_batches}) still go through
     * {@link JdbcTimestamps} directly — reshaping them is a phase of its own.
     */
    default Object splitTimestamp(Instant instant) {
        return JdbcTimestamps.toUtcLocalDateTime(instant);
    }

    default @Nullable Instant readSplitTimestamp(ResultSet rs, String column) throws SQLException {
        LocalDateTime value = rs.getObject(column, LocalDateTime.class);
        return value == null ? null : JdbcTimestamps.fromUtcLocalDateTime(value);
    }

    /**
     * The portable template for the claim's candidate sweep: single-shard per statement (a measured lesson —
     * a multi-shard predicate kills the index's ordering), ordered by {@code (priority, visible_at)},
     * which the {@code idx_mohs_ready_claim} index supplies without a Sort.
     *
     * <p>Shared by H2 and MySQL; SQL Server overrides {@link #selectReadyCandidates} ({@code TOP} plus a
     * lock hint); Postgres overrides all of {@link #claimReady} (a single statement). Two constants
     * rather than concatenation: a {@code NOT IN} over an empty list does not expand.
     */
    String ANSI_READY_CANDIDATES = """
            SELECT execution_id, job_key, attempt, priority
            FROM mohs_ready
            WHERE shard = :shard AND visible_at <= :now
            ORDER BY priority, visible_at
            LIMIT :limit
            FOR UPDATE SKIP LOCKED
            """;

    String ANSI_READY_CANDIDATES_FILTERED = """
            SELECT execution_id, job_key, attempt, priority
            FROM mohs_ready
            WHERE shard = :shard AND visible_at <= :now AND job_key NOT IN (:inadmissible)
            ORDER BY priority, visible_at
            LIMIT :limit
            FOR UPDATE SKIP LOCKED
            """;

    String READY_DELETE = "DELETE FROM mohs_ready WHERE execution_id IN (:ids)";

    String LEASE_INSERT = """
            INSERT INTO mohs_lease (execution_id, job_key, node_id, epoch, attempt_number, priority, claimed_at)
            VALUES (:executionId, :jobKey, :nodeId, :epoch, :attempt, :priority, :now)
            """;

    default List<ClaimedReady> selectReadyCandidates(NamedParameterJdbcTemplate jdbcTemplate, int shard, int limit,
            Collection<String> inadmissibleJobKeys, Instant now) {
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("shard", shard)
                .addValue("now", splitTimestamp(now))
                .addValue("limit", limit);
        if (inadmissibleJobKeys.isEmpty()) {
            return jdbcTemplate.query(ANSI_READY_CANDIDATES, params, ClaimedReady::fromReadyRow);
        }
        return jdbcTemplate.query(ANSI_READY_CANDIDATES_FILTERED,
                params.addValue("inadmissible", inadmissibleJobKeys), ClaimedReady::fromReadyRow);
    }

    /**
     * The claim: it removes from the queue and inserts the ownership — the CALLER
     * ({@code JdbcWorkQueue}) guarantees the transaction that makes both effects atomic (there is no
     * instant of "neither queued nor owned"). The default is portable, in three statements (a locking
     * SELECT, a DELETE, then a batched INSERT); Postgres overrides it with the single
     * {@code WITH picked … DELETE … RETURNING → INSERT} statement.
     */
    default List<ClaimedReady> claimReady(NamedParameterJdbcTemplate jdbcTemplate, int shard, String nodeId, long epoch,
            int limit, Collection<String> inadmissibleJobKeys, Instant now) {
        List<ClaimedReady> picked = selectReadyCandidates(jdbcTemplate, shard, limit, inadmissibleJobKeys, now);
        if (picked.isEmpty()) {
            return picked;
        }
        jdbcTemplate.update(READY_DELETE, new MapSqlParameterSource()
                .addValue("ids", picked.stream().map(ClaimedReady::executionId).toList()));
        MapSqlParameterSource[] leases = picked.stream()
                .map(row -> new MapSqlParameterSource()
                        .addValue("executionId", row.executionId())
                        .addValue("jobKey", row.jobKey())
                        .addValue("nodeId", nodeId)
                        .addValue("epoch", epoch)
                        .addValue("attempt", row.attempt())
                        .addValue("priority", row.priority())
                        .addValue("now", splitTimestamp(now)))
                .toArray(MapSqlParameterSource[]::new);
        jdbcTemplate.batchUpdate(LEASE_INSERT, leases);
        return picked;
    }
}
