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

import java.time.Instant;
import java.util.List;
import java.util.stream.Stream;
import org.springframework.core.io.ClassPathResource;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.jdbc.core.JdbcTemplate;
import tools.jackson.databind.json.JsonMapper;
import io.mohs.core.definition.JobOptions;
import io.mohs.core.execution.ExecutionId;
import io.mohs.core.execution.ExecutionState;
import io.mohs.core.job.JobKey;
import io.mohs.core.schedule.Misfire;
import io.mohs.core.schedule.OnDemandSpec;
import io.mohs.engine.LeaseStore;
import io.mohs.engine.JobDefinitions;
import io.mohs.engine.WorkQueue;
import io.mohs.store.jdbc.JdbcJobBindingTest.Backend;
import io.mohs.test.MutableClock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JdbcLastAttemptTest {
    private static final Instant NOW = Instant.parse("2026-09-08T12:00:00Z");
    private static final JobKey JOB = JobKey.of("latest-attempt-job");
    private static final ExecutionId ID = ExecutionId.of("01991337-0000-7000-8000-000000000058");

    @ParameterizedTest(name = "{0}")
    @MethodSource("io.mohs.store.jdbc.JdbcJobBindingTest#backends")
    void installerHasEightTablesIncludingCatalog(Backend backend) throws Exception {
        try (var connection = backend.dataSource().getConnection();
             var tables = connection.getMetaData().getTables(connection.getCatalog(), null, "%", new String[] {"TABLE"})) {
            var names = new java.util.ArrayList<String>();
            while (tables.next()) {
                String name = tables.getString("TABLE_NAME").toLowerCase(java.util.Locale.ROOT);
                if (name.startsWith("mohs_")) names.add(name);
            }
            assertThat(names).hasSize(8).contains("mohs_catalog", "mohs_work")
                    .doesNotContain("mohs_ready", "mohs_lease", "mohs_attempt", "mohs_job_bindings");
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("io.mohs.store.jdbc.JdbcJobBindingTest#backends")
    void failuresThenSuccessKeepOneLatestResultAndManualRetryAdvancesItsNumber(Backend backend) {
        var f = fixture(backend);
        seed(f);
        assertLatest(f, 0, null, null);
        for (int attempt = 1; attempt <= 2; attempt++) {
            if (attempt == 2) assertThat(f.queue.rearmForManualRetry(ID, NOW)).isTrue();
            assertThat(claim(f)).hasSize(1);
            assertThat(f.leases.complete(List.of(result(f,attempt, ExecutionState.FAILED, true))).get(ID).owned()).isTrue();
            assertLatest(f, attempt, "FAILED", "failure-" + attempt);
        }
        assertThat(f.queue.rearmForManualRetry(ID, NOW)).isTrue();
        assertThat(f.jdbc.queryForObject("SELECT attempt FROM mohs_work WHERE work_state = 'READY' AND  execution_id = ?", Integer.class, ID.value())).isEqualTo(3);
        assertThat(f.history.countTerminalOutcomesSince(NOW.minusSeconds(1))).isEmpty();
        claim(f);
        assertThat(f.leases.complete(List.of(result(f,3, ExecutionState.SUCCEEDED, true))).get(ID).owned()).isTrue();
        assertLatest(f, 3, "SUCCEEDED", null);
        assertThat(f.jdbc.queryForObject("SELECT last_attempt_error_type FROM mohs_execution WHERE execution_id = ?", String.class, ID.value())).isNull();
        assertThat(f.history.countTerminalOutcomesSince(NOW.minusSeconds(1))).containsEntry(ExecutionState.SUCCEEDED, 1L);
        assertThat(f.history.countTerminalOutcomesSince(NOW.minusSeconds(1)).getOrDefault(ExecutionState.FAILED, 0L)).isZero();
        assertThat(f.leases.complete(List.of(result(f,2, ExecutionState.FAILED, true))).get(ID)).isEqualTo(LeaseStore.Completion.FENCED_OUT);
        assertLatest(f, 3, "SUCCEEDED", null);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("io.mohs.store.jdbc.JdbcJobBindingTest#backends")
    void retryableFailureIsVisibleWhileWaitingAndFailedCompletionRollsBack(Backend backend) {
        var f = fixture(backend);
        seed(f);
        claim(f);
        f.leases.complete(List.of(result(f,1, ExecutionState.FAILED, false)));
        assertLatest(f, 1, "FAILED", "failure-1");
        assertThat(f.jdbc.queryForObject("SELECT state FROM mohs_execution WHERE execution_id = ?", String.class, ID.value())).isEqualTo("PENDING");
        assertThat(f.history.countTerminalOutcomesSince(NOW.minusSeconds(1))).isEmpty();
        claim(f);
        // Lose the response after the actual fenced work release and latest-attempt UPDATE.
        var failing = new JdbcLeaseStore(new TestJdbcFaults(f.jdbc.getDataSource(), "UPDATE mohs_execution SET"), f.dialect);
        assertThatThrownBy(() -> failing.complete(List.of(result(f,2, ExecutionState.FAILED, false))))
                .isInstanceOf(DataAccessException.class);
        assertLatest(f, 1, "FAILED", "failure-1");
        assertThat(f.jdbc.queryForObject("SELECT attempt FROM mohs_work WHERE work_state = 'LEASED' AND  execution_id = ?", Integer.class, ID.value())).isEqualTo(2);
        f.leases.complete(List.of(result(f,2, ExecutionState.SUCCEEDED, true)));
        assertLatest(f, 2, "SUCCEEDED", null);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("io.mohs.store.jdbc.JdbcJobBindingTest#backends")
    void queuedCancellationDoesNotInventACompletedAttempt(Backend backend) {
        var f = fixture(backend);
        seed(f);
        assertThat(f.queue.cancel(ID, NOW)).isTrue();
        assertThat(f.history.find(ID, NOW).orElseThrow().state()).isEqualTo(ExecutionState.CANCELLED);
        assertLatest(f, 0, null, null);
        assertThat(f.history.countTerminalOutcomesSince(NOW.minusSeconds(1))).isEmpty();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("io.mohs.store.jdbc.JdbcJobBindingTest#backends")
    void constraintRejectsIncompleteAttemptAndUnknownOutcome(Backend backend) {
        var f = fixture(backend);
        seed(f);
        for (String invalid : List.of("last_attempt_number = -1", "last_attempt_number = 1",
                "last_attempt_outcome = 'RUNNING'")) {
            assertThatThrownBy(() -> f.jdbc.update("UPDATE mohs_execution SET " + invalid + " WHERE execution_id = ?", ID.value()))
                    .isInstanceOf(DataAccessException.class).hasMessageMatching("(?is).*ck_mohs_execution_attempt.*");
        }
        assertLatest(f, 0, null, null);
    }

    private static LeaseStore.CompletionResult result(Fixture f, int number, ExecutionState outcome, boolean terminal) {
        return new LeaseStore.CompletionResult(ID, JOB, "01991337-0000-7000-8000-000000000063", f.tokens.getOrDefault(number, "01991337-0000-7000-8000-000000000085"), number, NOW.minusSeconds(1), NOW,
                outcome, outcome == ExecutionState.FAILED ? "java.lang.IllegalStateException" : null,
                outcome == ExecutionState.FAILED ? "failure-" + number : null, terminal ? null : NOW);
    }

    private static List<WorkQueue.ClaimedWork> claim(Fixture f) {
        var claimed = f.queue.claim(0, "01991337-0000-7000-8000-000000000063", 1, List.of(), NOW);
        claimed.forEach(w -> f.tokens.put(w.attemptNumber(), w.claimToken()));
        return claimed;
    }

    private static void seed(Fixture f) {
        f.jobs.upsert(JobDefinitions.definition(JOB, JobOptions.defaults(), new OnDemandSpec(), Misfire.IGNORE, false));
        f.jdbc.update("""
                INSERT INTO mohs_execution (execution_id, job_key, state, scheduled_at, created_at, actor, payload, input_contract)
                VALUES (?, ?, 'PENDING', ?, ?, 'test', '{}', 'java.lang.Object')
                """, ID.value(), JOB.value(), JdbcTimestamps.toUtcLocalDateTime(NOW), JdbcTimestamps.toUtcLocalDateTime(NOW));
        TestWork.ready(f.jdbc.getDataSource(), f.dialect, List.of(new WorkQueue.ReadyEntry(ID, JOB, 0, 20, 1, NOW)));
    }

    private static void assertLatest(Fixture f, int number, @org.jspecify.annotations.Nullable String outcome, @org.jspecify.annotations.Nullable String error) {
        var row = f.jdbc.queryForMap("""
                SELECT last_attempt_number, last_attempt_outcome, last_attempt_error
                FROM mohs_execution WHERE execution_id = ?
                """, ID.value());
        assertThat(((Number) row.get("last_attempt_number")).intValue()).isEqualTo(number);
        assertThat(row.get("last_attempt_outcome")).isEqualTo(outcome);
        assertThat(row.get("last_attempt_error")).isEqualTo(error);
    }

    private static Fixture fixture(Backend backend) {
        var clock = MutableClock.startingAt(NOW);
        TestSessions.seed(backend.dataSource(), backend.delegate(), "01991337-0000-7000-8000-000000000063", io.mohs.core.EngineState.RUNNING, java.time.Duration.ofMinutes(5));
        var batches = new JdbcBatchStore(backend.dataSource(), backend.delegate());
        return new Fixture(new JdbcTemplate(backend.dataSource()),
                new JdbcJobStore(backend.dataSource(), clock, backend.delegate()),
                new JdbcWorkQueue(backend.dataSource(), backend.delegate()),
                new JdbcLeaseStore(backend.dataSource(), backend.delegate()),
                new JdbcHistoryStore(backend.dataSource(), backend.delegate()), new java.util.HashMap<>(), backend.delegate());
    }

    private record Fixture(JdbcTemplate jdbc, JdbcJobStore jobs, JdbcWorkQueue queue,
                           JdbcLeaseStore leases, JdbcHistoryStore history, java.util.Map<Integer, String> tokens, io.mohs.store.jdbc.delegate.JdbcDelegate dialect) {}
}
