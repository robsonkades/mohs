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
import io.mohs.store.jdbc.delegate.JdbcDelegate;

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
     * A ceiling on WAITING for a statement the engine's loop thread issues — the heartbeat, the reads
     * behind the reaper and the reconcile, the cancel poll, the firing CAS, the claim and the
     * requeue. That thread also carries the node lease: a statement blocked on another node's locks
     * is a node that is alive, RUNNING, owning its shards and claiming nothing, until the promise
     * expires and its peers reap the work it is still running.
     *
     * <p>Three seconds is a quarter of the 12-second floor validated on {@code node-lease-ttl} at
     * boot, and deliberately not derived from the configured TTL: every statement under it is bounded
     * by the work in flight (a claim batch, one node's leases, the cluster's nodes), so one that has
     * not answered in three seconds is waiting on a lock, not reading rows, and a longer promise
     * would only loosen the ceiling. That is also why the definition scans are NOT under it: their
     * cost is rows transferred (2.8 s measured for 1M definitions), and a ceiling on them would kill
     * the tick every cycle. Statements a HOST thread issues — the enqueue, the completion flush, the
     * execution history's reads — keep their own templates with no timeout: cancelling a caller's
     * write mid-flight is not this ceiling's business. The one host read that shares a tick template
     * is the facade's node listing, one row per node, which costs it nothing.
     *
     * <p>On H2 the timeout is session state ({@code SET QUERY_TIMEOUT}), so it outlives the
     * statement and follows the pooled connection to the next template — accepted for the dev
     * dialect. The three production drivers keep it per statement.
     */
    static final int TICK_STATEMENT_TIMEOUT_SECONDS = 3;

    private JdbcSupport() {
    }

    /** A {@code NamedParameterJdbcTemplate} with {@link #STREAM_FETCH_SIZE} — every store with a {@code queryForStream} method uses this constructor, not {@code new NamedParameterJdbcTemplate(dataSource)} directly. */
    static NamedParameterJdbcTemplate namedTemplateWithStreamFetchSize(DataSource dataSource) {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        jdbcTemplate.setFetchSize(STREAM_FETCH_SIZE);
        return new NamedParameterJdbcTemplate(jdbcTemplate);
    }

    /** The template for statements issued on the engine's loop thread — see {@link #TICK_STATEMENT_TIMEOUT_SECONDS}. */
    static NamedParameterJdbcTemplate tickTemplate(DataSource dataSource) {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        jdbcTemplate.setQueryTimeout(TICK_STATEMENT_TIMEOUT_SECONDS);
        return new NamedParameterJdbcTemplate(jdbcTemplate);
    }

    /** Chunks of at most {@link #MAX_IDS_PER_QUERY} ids for {@code IN (:ids)} — {@code subList} views, with no copying. */
    static List<List<String>> chunksOf(List<String> ids) {
        List<List<String>> chunks = new ArrayList<>();
        for (int start = 0; start < ids.size(); start += MAX_IDS_PER_QUERY) {
            chunks.add(ids.subList(start, Math.min(start + MAX_IDS_PER_QUERY, ids.size())));
        }
        return chunks;
    }

    /** {@link JdbcDelegate#readyInsert()}'s parameters — {@code visibleAt}'s temporal crossing belongs to the delegate ({@code splitTimestamp}). */
    static MapSqlParameterSource readyEntryParams(WorkQueue.ReadyEntry entry, JdbcDelegate delegate) {
        return new MapSqlParameterSource()
                .addValue("executionId", entry.executionId().value())
                .addValue("jobKey", entry.jobKey().value())
                .addValue("shard", entry.shard())
                .addValue("priority", entry.priority())
                .addValue("attempt", entry.attempt())
                .addValue("visibleAt", delegate.splitTimestamp(entry.visibleAt()));
    }

    /** {@link JdbcDelegate#fencedLeaseDelete()}'s parameters — the whole fencing token, attempt included. */
    static MapSqlParameterSource fencedLeaseDeleteParams(String executionId, String nodeId, long epoch, int attemptNumber) {
        return new MapSqlParameterSource()
                .addValue("executionId", executionId)
                .addValue("nodeId", nodeId)
                .addValue("epoch", epoch)
                .addValue("attemptNumber", attemptNumber);
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
