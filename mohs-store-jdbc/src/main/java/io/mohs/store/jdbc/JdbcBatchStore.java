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
    private final JdbcDelegate delegate;

    /**
     * Creates a {@code JdbcBatchStore} with the supplied values.
     *
     * @param dataSource the configured database connection source
     * @param delegate the database-specific SQL and timestamp adapter
     */
    public JdbcBatchStore(DataSource dataSource, JdbcDelegate delegate) {
        this.jdbcTemplate = new NamedParameterJdbcTemplate(Objects.requireNonNull(dataSource, "dataSource"));
        this.delegate = Objects.requireNonNull(delegate, "delegate");
    }

    @Override
    public Optional<BatchCounters> find(String batchId) {
        Objects.requireNonNull(batchId, "batchId");
        return JdbcSupport.findOne(jdbcTemplate, delegate.findBatch(),
                new MapSqlParameterSource("id", batchId),
                JdbcBatchStore::mapRow);
    }

    static BatchCounters mapRow(ResultSet rs) throws SQLException {
        return new BatchCounters(rs.getString("id"), rs.getString("name"), rs.getLong("total"), rs.getLong("succeeded"),
                rs.getLong("failed"));
    }
}
