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

import java.time.Instant;
import java.util.Collection;
import java.util.List;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

/**
 * SQL Server: no {@code LIMIT} — it uses {@code TOP (:batchSize)} right after {@code SELECT}, changing
 * the position in the query rather than only the text. No {@code SKIP LOCKED} — it uses the table hint
 * {@code WITH (UPDLOCK, ROWLOCK, READPAST)}, confirmed through jOOQ (it is what jOOQ generates to
 * emulate {@code SKIP LOCKED} on SQL Server). {@code BIT} compares against {@code 1}, not
 * {@code TRUE}.
 */
public final class SqlServerJdbcDialect implements JdbcDialect {

    @Override
    public String migrationLocation() {
        return "classpath:io/mohs/store/jdbc/migration/sqlserver";
    }

    @Override
    public String topClause() {
        return "TOP (:limit) ";
    }

    @Override
    public String limitClause() {
        return "";
    }

    /**
     * The claim's candidate sweep in T-SQL form: {@code TOP} plus a table hint in place of
     * {@code LIMIT … FOR UPDATE SKIP LOCKED} — the emulation jOOQ confirms (see the class Javadoc). The
     * rest of the claim (the ownership's DELETE and INSERT) follows the interface's portable default.
     *
     * <p>Two constants because a {@code NOT IN} over an empty list does not expand — the filtered one
     * derived through {@code replace}, like Postgres's {@code CLAIM_READY_FILTERED}.
     */
    private static final String TSQL_READY_CANDIDATES = """
            SELECT TOP (:limit) execution_id, job_key, attempt, priority
            FROM mohs_ready WITH (UPDLOCK, ROWLOCK, READPAST)
            WHERE shard = :shard AND visible_at <= :now
            ORDER BY priority, visible_at
            """;

    private static final String TSQL_READY_CANDIDATES_FILTERED = TSQL_READY_CANDIDATES.replace(
            "WHERE shard = :shard AND visible_at <= :now",
            "WHERE shard = :shard AND visible_at <= :now AND job_key NOT IN (:inadmissible)");

    static {
        // A guard on the replace: if the WHERE anchor changes and the replace becomes a no-op,
        // the inadmissible filter would vanish in silence
        if (!TSQL_READY_CANDIDATES_FILTERED.contains(":inadmissible")) {
            throw new ExceptionInInitializerError("TSQL_READY_CANDIDATES_FILTERED lost its :inadmissible predicate — the replace anchor drifted");
        }
    }

    @Override
    public List<ClaimedReady> selectReadyCandidates(NamedParameterJdbcTemplate jdbcTemplate, int shard, int limit,
            Collection<String> inadmissibleJobKeys, Instant now) {
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("shard", shard)
                .addValue("now", splitTimestamp(now))
                .addValue("limit", limit);
        if (inadmissibleJobKeys.isEmpty()) {
            return jdbcTemplate.query(TSQL_READY_CANDIDATES, params, ClaimedReady::fromReadyRow);
        }
        return jdbcTemplate.query(TSQL_READY_CANDIDATES_FILTERED,
                params.addValue("inadmissible", inadmissibleJobKeys), ClaimedReady::fromReadyRow);
    }

    /**
     * {@code NOLOCK} (read uncommitted), not {@code READPAST}: skipping a locked row systematically
     * undercounts under load.
     *
     * <p>The accepted error is the mechanism's worst case, not merely "±1 in transition": with no
     * required order ({@code COUNT}/{@code GROUP BY}) the optimiser may choose an allocation-order scan,
     * which under a concurrent page split counts a row twice or loses it — an error proportional to
     * write churn; and the scan may fail with error 601 ("data movement"), which here becomes a
     * transient read failure (a 500 on the GET; a WARN plus a retry on the next tick in the stream) —
     * accepted, with no automatic retry.
     *
     * <p>A deployment with RCSI ({@code READ_COMMITTED_SNAPSHOT ON}) makes the hint redundant — the
     * operator's decision, not the library's.
     */
    @Override
    public String lockFreeReadHint() {
        return "WITH (NOLOCK) ";
    }
}
