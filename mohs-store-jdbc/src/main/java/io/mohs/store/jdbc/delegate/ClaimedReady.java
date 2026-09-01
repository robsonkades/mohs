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
package io.mohs.store.jdbc.delegate;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * One row claimed from the queue ({@code mohs_ready} to {@code mohs_lease}) — identity and the attempt
 * the entry becomes, never the payload (the dispatcher follows with ONE batched read of history).
 */
public record ClaimedReady(String executionId, String jobKey, int attempt, int priority) {

    /** A {@code mohs_ready} row (the {@code attempt}/{@code priority} columns) — one mapping for all four delegates: Postgres's single statement also returns from the ordered SELECT over {@code picked}, not from the lease's {@code RETURNING}. */
    static ClaimedReady fromReadyRow(ResultSet rs, int rowNum) throws SQLException {
        return new ClaimedReady(rs.getString("execution_id"), rs.getString("job_key"), rs.getInt("attempt"), rs.getInt("priority"));
    }
}
