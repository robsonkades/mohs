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
import java.time.Clock;
import java.util.Objects;
import java.util.Optional;

import javax.sql.DataSource;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import io.mohs.engine.BatchCounters;
import io.mohs.engine.BatchStore;
import io.mohs.store.jdbc.delegate.JdbcDelegate;

/** {@link BatchStore} over {@code mohs_batches} (a Data Mapper, PoEAA). */
public final class JdbcBatchStore implements BatchStore {

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final Clock clock;
    private final JdbcDelegate delegate;

    /**
     * Creates a {@code JdbcBatchStore} with the supplied values.
     *
     * @param dataSource the configured database connection source
     * @param clock the time source used by the component
     * @param delegate the database-specific SQL and timestamp adapter
     */
    public JdbcBatchStore(DataSource dataSource, Clock clock, JdbcDelegate delegate) {
        this.jdbcTemplate = new NamedParameterJdbcTemplate(Objects.requireNonNull(dataSource, "dataSource"));
        this.clock = Objects.requireNonNull(clock, "clock");
        this.delegate = Objects.requireNonNull(delegate, "delegate");
    }

    @Override
    public void insert(String batchId, String name, int total) {
        Objects.requireNonNull(batchId, "batchId");
        Objects.requireNonNull(name, "name");
        if (total < 0) {
            throw new IllegalArgumentException("total must not be negative");
        }
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("id", batchId)
                .addValue("name", name)
                .addValue("total", total)
                .addValue("createdAt", JdbcTimestamps.toUtcLocalDateTime(clock.instant()));
        jdbcTemplate.update(delegate.insertBatch(), params);
    }

    @Override
    public Optional<BatchCounters> find(String batchId) {
        Objects.requireNonNull(batchId, "batchId");
        return JdbcSupport.findOne(jdbcTemplate, delegate.findBatch(),
                new MapSqlParameterSource("id", batchId),
                JdbcBatchStore::mapRow);
    }

    @Override
    public BatchCounters incrementSucceeded(String batchId) {
        return incrementAndRead(batchId, delegate.incrementBatchSucceeded());
    }

    @Override
    public BatchCounters incrementFailed(String batchId) {
        return incrementAndRead(batchId, delegate.incrementBatchFailed());
    }

    /**
     * An {@code UPDATE} followed by a {@code SELECT}, rather than the {@code UPDATE ... RETURNING} the
     * design names: H2 does not support the clause (measured while implementing) and neither does MySQL,
     * so this is the only path that serves all four databases. One extra round trip on Postgres and SQL
     * Server is the price, and the measurement showed the candidates separated by less than a round trip
     * — optimising per delegate before having a number that justifies it would be premature
     * generalisation.
     *
     * <p>The re-read is stable because the {@code UPDATE}'s row lock is held until the commit: no other
     * transaction can commit over the row between the two statements, so what comes back is exactly what
     * THIS transaction wrote. That depends on both living in the SAME transaction — which the completion
     * guarantees. Outside one, two concurrent completions could read the same final balance and both
     * believe they closed the batch.
     */
    private BatchCounters incrementAndRead(String batchId, String incrementSql) {
        Objects.requireNonNull(batchId, "batchId");
        MapSqlParameterSource params = new MapSqlParameterSource("id", batchId);
        if (jdbcTemplate.update(incrementSql, params) == 0) {
            // A REAL guard: mohs_execution.correlation_id has no foreign key (that died with the old
            // layout) — an orphan batchId reaches here, and counting it silently would lose the batch's
            // completion forever.
            throw new IllegalStateException("no batch '" + batchId + "' to count a member into");
        }
        return find(batchId).orElseThrow(() ->
                new IllegalStateException("batch '" + batchId + "' vanished between increment and read"));
    }

    private static BatchCounters mapRow(ResultSet rs) throws SQLException {
        return new BatchCounters(rs.getString("id"), rs.getString("name"), rs.getInt("total"), rs.getInt("succeeded"),
                rs.getInt("failed"));
    }
}
