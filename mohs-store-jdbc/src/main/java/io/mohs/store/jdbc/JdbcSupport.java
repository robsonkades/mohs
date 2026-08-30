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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import javax.sql.DataSource;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import io.mohs.engine.WorkQueue;
import io.mohs.store.jdbc.dialect.JdbcDialect;

/**
 * JDBC conventions shared between the {@code io.mohs.store.jdbc} stores — the same idiom already used in
 * {@code io.mohs.cron} ({@code Assert}/{@code StringUtils}): a package-private final class with static
 * methods.
 */
final class JdbcSupport {

    /**
     * Without this, {@code queryForStream} is not a real cursor in the actual drivers — pgjdbc
     * materialises the entire query in memory during {@code executeQuery} without a
     * {@code fetchSize > 0} (and it still requires {@code autoCommit = false} to take effect;
     * Connector/J needs the same, or {@code useCursorFetch=true}).
     *
     * <p>The value is a middle ground (100-500 is the recommended range) — never measured in this
     * project; it only closes the gap between {@code HistoryStore#findAll}'s Javadoc and what the drivers
     * actually do without it configured.
     */
    static final int STREAM_FETCH_SIZE = 200;

    /** Well below SQL Server's 2100-parameter ceiling for {@code IN (:ids)} — one node's in-flight work easily exceeds 1k ids. */
    static final int MAX_IDS_PER_QUERY = 1000;

    /**
     * The {@code INSERT} into {@code mohs_ready} — enqueue, retry and requeue are the SAME operation with
     * a different {@code visibleAt}: {@code JdbcWorkQueue#offer}/{@code #requeue} and the retry's rebirth
     * inside {@code JdbcLeaseStore#complete}'s completion transaction share this statement and
     * {@link #readyEntryParams}.
     */
    static final String READY_INSERT = """
            INSERT INTO mohs_ready (execution_id, job_key, shard, priority, attempt, visible_at)
            VALUES (:executionId, :jobKey, :shard, :priority, :attempt, :visibleAt)
            """;

    /**
     * The {@code DELETE} from {@code mohs_lease} fenced by {@code (node_id, epoch)} — the fencing token:
     * the lease only drops if it still belongs to the observed incarnation.
     *
     * <p>The SAME statement decides the fence in the requeue ({@code JdbcWorkQueue}) and in the
     * completion ({@code JdbcLeaseStore}) — shared so the semantics can never diverge between the two.
     */
    static final String FENCED_LEASE_DELETE = """
            DELETE FROM mohs_lease
            WHERE execution_id = :executionId AND node_id = :nodeId AND epoch = :epoch
            """;

    private JdbcSupport() {
    }

    /** Chunks of at most {@link #MAX_IDS_PER_QUERY} ids for {@code IN (:ids)} — {@code subList} views, with no copying. */
    static List<List<String>> chunksOf(List<String> ids) {
        List<List<String>> chunks = new ArrayList<>();
        for (int start = 0; start < ids.size(); start += MAX_IDS_PER_QUERY) {
            chunks.add(ids.subList(start, Math.min(start + MAX_IDS_PER_QUERY, ids.size())));
        }
        return chunks;
    }

    /** {@link #READY_INSERT}'s parameters — {@code visibleAt}'s temporal crossing belongs to the dialect ({@code splitTimestamp}). */
    static MapSqlParameterSource readyEntryParams(WorkQueue.ReadyEntry entry, JdbcDialect dialect) {
        return new MapSqlParameterSource()
                .addValue("executionId", entry.executionId().value())
                .addValue("jobKey", entry.jobKey().value())
                .addValue("shard", entry.shard())
                .addValue("priority", entry.priority())
                .addValue("attempt", entry.attempt())
                .addValue("visibleAt", dialect.splitTimestamp(entry.visibleAt()));
    }

    /** {@link #FENCED_LEASE_DELETE}'s parameters. */
    static MapSqlParameterSource fencedLeaseDeleteParams(String executionId, String nodeId, long epoch) {
        return new MapSqlParameterSource()
                .addValue("executionId", executionId)
                .addValue("nodeId", nodeId)
                .addValue("epoch", epoch);
    }

    /** A {@code NamedParameterJdbcTemplate} with {@link #STREAM_FETCH_SIZE} — every store with a {@code queryForStream} method uses this constructor, not {@code new NamedParameterJdbcTemplate(dataSource)} directly. */
    static NamedParameterJdbcTemplate namedTemplateWithStreamFetchSize(DataSource dataSource) {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        jdbcTemplate.setFetchSize(STREAM_FETCH_SIZE);
        return new NamedParameterJdbcTemplate(jdbcTemplate);
    }

    @FunctionalInterface
    interface SingleRowMapper<T> {
        T map(ResultSet rs) throws SQLException;
    }

    /**
     * Fetches one row by a condition that matches at most one (a primary or unique key) — a
     * {@code ResultSetExtractor} guarded by {@code rs.next()} reads that row directly, without going
     * through a {@code List}/{@code stream}/{@code findFirst}, and without {@code queryForObject}'s
     * hazard (which throws {@code EmptyResultDataAccessException} instead of returning empty when there
     * is no row at all).
     */
    static <T> Optional<T> findOne(NamedParameterJdbcTemplate jdbcTemplate, String sql, MapSqlParameterSource params, SingleRowMapper<T> mapper) {
        T result = jdbcTemplate.query(sql, params, rs -> rs.next() ? mapper.map(rs) : null);
        return Optional.ofNullable(result);
    }
}
