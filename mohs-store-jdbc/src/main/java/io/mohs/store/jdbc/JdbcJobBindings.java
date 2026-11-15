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
import java.util.Optional;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import io.mohs.core.job.JobKey;
import io.mohs.engine.JobBinding;
import io.mohs.engine.JobPayload;
import io.mohs.store.jdbc.delegate.JdbcDelegate;

/** Shared binding reads; the firing path invokes these only after locking its owning definition. */
final class JdbcJobBindings {
    private JdbcJobBindings() {}

    static Optional<JobBinding> find(NamedParameterJdbcTemplate jdbc, JdbcDelegate delegate, JobKey key) {
        return JdbcSupport.findOne(jdbc, delegate.findJobBinding(),
                new MapSqlParameterSource("jobKey", key.value()), JdbcJobBindings::map);
    }

    static JobBinding map(ResultSet rs) throws SQLException {
        String json = rs.getString("recurring_payload");
        JobPayload input = json == null ? null : new JobPayload(json, rs.getString("input_contract"));
        return new JobBinding(JobKey.of(rs.getString("job_key")), JobKey.of(rs.getString("executable_key")), input);
    }
}
