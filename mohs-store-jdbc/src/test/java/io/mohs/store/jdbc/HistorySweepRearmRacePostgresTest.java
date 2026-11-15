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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.jdbc.core.JdbcTemplate;

import io.mohs.engine.HistoryStore;
import io.mohs.store.jdbc.delegate.PostgresJdbcDelegate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * A retry can leave an old terminal row pending or complete it again after the sweep selects its id.
 * PostgreSQL re-evaluates the DELETE's outer predicates against the current row under the lock.
 * Both state and finished_at must be checked there to retain the pending or recent terminal result.
 *
 * <p>Deterministic by lock sequencing, not sleeps: the rearm holds the row lock uncommitted, the
 * sweep provably blocks on it ({@code pg_locks} shows an ungranted lock), the rearm commits, and the
 * sweep's re-evaluation must spare the current row.
 */
@Tag("docker")
class HistorySweepRearmRacePostgresTest {

    private static final Instant NOW = Instant.parse("2026-08-22T12:00:00Z");
    private static final Instant OLD = NOW.minus(Duration.ofDays(60));

    @ParameterizedTest(name = "a current {0} result survives an old sweep candidate")
    @ValueSource(strings = {"PENDING", "SUCCEEDED", "FAILED", "CANCELLED"})
    void aRowRearmedOrCompletedAgainMidSweepSurvivesIt(String state) throws Exception {
        DataSource dataSource = PostgresTestSupport.freshSchema();
        PostgresJdbcDelegate delegate = new PostgresJdbcDelegate();
        JdbcHistoryStore store = new JdbcHistoryStore(dataSource, delegate);
        JdbcTemplate raw = new JdbcTemplate(dataSource);
        long ms = OLD.toEpochMilli();
        String id = "%08x-%04x-7fff-8fff-000000000001".formatted(ms >>> 16, ms & 0xFFFF);
        raw.update("""
                INSERT INTO mohs_execution (execution_id, job_key, shard, priority, state, scheduled_at, created_at,
                    finished_at, actor, payload, input_contract)
                VALUES (?, 'job-a', 0, 20, 'FAILED', ?, ?, ?, 'test', '{}', 'x')
                """, id, delegate.splitTimestamp(OLD), delegate.splitTimestamp(OLD), delegate.splitTimestamp(OLD));

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (Connection rearm = dataSource.getConnection()) {
            rearm.setAutoCommit(false);
            try (PreparedStatement update = rearm.prepareStatement(
                    "UPDATE mohs_execution SET state = ?, finished_at = ? WHERE execution_id = ? AND state = 'FAILED'")) {
                update.setString(1, state);
                update.setObject(2, state.equals("PENDING") ? null : delegate.splitTimestamp(NOW));
                update.setString(3, id);
                assertThat(update.executeUpdate()).isEqualTo(1);
            }

            Future<HistoryStore.PrunedHistory> sweep = executor.submit(
                    () -> store.pruneHistoryBefore(NOW.minus(Duration.ofDays(30)), 100));
            // The sweep is provably at the lock before the rearm commits — the ordering the race needs.
            // Polled fast, because the sweep statement carries a 1s query timeout of its own.
            await().atMost(Duration.ofSeconds(5)).pollInterval(Duration.ofMillis(25)).until(() -> Boolean.TRUE.equals(
                    raw.queryForObject("SELECT EXISTS (SELECT 1 FROM pg_locks WHERE NOT granted)", Boolean.class)));
            rearm.commit();

            assertThat(sweep.get(5, TimeUnit.SECONDS).executions()).isZero();
        } finally {
            executor.shutdownNow();
        }

        assertThat(raw.queryForObject("SELECT state FROM mohs_execution WHERE execution_id = ?", String.class, id))
                .isEqualTo(state);
    }
}
