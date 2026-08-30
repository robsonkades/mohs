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
import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.List;

import org.jspecify.annotations.Nullable;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import io.mohs.store.jdbc.JdbcTimestamps;

/** PostgreSQL — {@code LIMIT} at the end, native {@code SKIP LOCKED} (this is where the syntax comes from). */
public final class PostgresJdbcDialect implements JdbcDialect {

    @Override
    public String migrationLocation() {
        return "classpath:io/mohs/store/jdbc/migration/postgresql";
    }

    /** The split tables are {@code TIMESTAMPTZ} here: UTC {@code OffsetDateTime} — a {@code LocalDateTime} would be read in the SESSION's zone (see the interface's Javadoc). */
    @Override
    public Object splitTimestamp(Instant instant) {
        return JdbcTimestamps.toUtcOffsetDateTime(instant);
    }

    @Override
    public @Nullable Instant readSplitTimestamp(ResultSet rs, String column) throws SQLException {
        OffsetDateTime value = rs.getObject(column, OffsetDateTime.class);
        return value == null ? null : JdbcTimestamps.fromUtcOffsetDateTime(value);
    }

    /**
     * The claim in the form only Postgres has: ONE statement — the CTE selects with
     * {@code SKIP LOCKED}, the {@code DELETE … USING} consumes the queue, the {@code INSERT} (also a
     * CTE) writes the ownership, and the final SELECT returns IN {@code (priority, visible_at)} ORDER —
     * an INSERT's {@code RETURNING} order is not guaranteed, and the other dialects' portable form
     * returns ordered: the port's contract is one and the same across all four.
     *
     * <p>The INSERT reads from {@code picked} — identical to reading from {@code gone}, which deletes
     * exactly {@code picked}. Two constants (with and without the inadmissible filter) because a
     * {@code NOT IN} over an empty list does not expand.
     */
    public static final String CLAIM_READY = """
            WITH picked AS (
                SELECT execution_id, job_key, attempt, priority, visible_at
                  FROM mohs_ready
                 WHERE shard = :shard AND visible_at <= :now
                 ORDER BY priority, visible_at
                 LIMIT :limit
                   FOR UPDATE SKIP LOCKED
            ),
            gone AS (
                DELETE FROM mohs_ready r USING picked p
                 WHERE r.execution_id = p.execution_id
            ),
            leased AS (
                INSERT INTO mohs_lease (execution_id, job_key, node_id, epoch, attempt_number, priority, claimed_at)
                SELECT execution_id, job_key, :nodeId, :epoch, attempt, priority, :now FROM picked
            )
            SELECT execution_id, job_key, attempt, priority FROM picked
            ORDER BY priority, visible_at
            """;

    public static final String CLAIM_READY_FILTERED = CLAIM_READY.replace(
            "WHERE shard = :shard AND visible_at <= :now",
            "WHERE shard = :shard AND visible_at <= :now AND job_key NOT IN (:inadmissible)");

    static {
        // A guard on the replace: if the WHERE anchor changes and the replace becomes a no-op,
        // the inadmissible filter would vanish in silence
        if (!CLAIM_READY_FILTERED.contains(":inadmissible")) {
            throw new ExceptionInInitializerError("CLAIM_READY_FILTERED lost its :inadmissible predicate — the replace anchor drifted");
        }
    }

    @Override
    public List<ClaimedReady> claimReady(NamedParameterJdbcTemplate jdbcTemplate, int shard, String nodeId, long epoch,
            int limit, Collection<String> inadmissibleJobKeys, Instant now) {
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("shard", shard)
                .addValue("now", splitTimestamp(now))
                .addValue("limit", limit)
                .addValue("nodeId", nodeId)
                .addValue("epoch", epoch);
        if (inadmissibleJobKeys.isEmpty()) {
            return jdbcTemplate.query(CLAIM_READY, params, ClaimedReady::fromReadyRow);
        }
        return jdbcTemplate.query(CLAIM_READY_FILTERED,
                params.addValue("inadmissible", inadmissibleJobKeys), ClaimedReady::fromReadyRow);
    }
}
