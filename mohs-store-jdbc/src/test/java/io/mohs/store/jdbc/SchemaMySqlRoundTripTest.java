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

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.Optional;

import javax.sql.DataSource;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;

import tools.jackson.databind.json.JsonMapper;

import io.mohs.core.definition.JobDefinition;
import io.mohs.core.execution.ExecutionId;
import io.mohs.core.job.JobKey;
import io.mohs.core.RateLimitSnapshot;
import io.mohs.core.resource.RateLimit;
import io.mohs.engine.BatchCounters;
import io.mohs.engine.HistoryStore;
import io.mohs.engine.StoredJob;
import io.mohs.store.jdbc.delegate.MySqlJdbcDelegate;
import io.mohs.test.MutableClock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Proves that the schema (all of it, not only what {@code JdbcWorkQueue} touches) and each store's DML
 * round-trip against a real MySQL, not just H2 and Postgres — in particular
 * {@code mohs_execution.payload}/{@code mohs_attempt.error} ({@code TEXT}) and the {@code DATETIME}
 * columns (MySQL does not use {@code TIMESTAMP} — see schema-mysql.sql).
 */
class SchemaMySqlRoundTripTest {

    record Handler() {
    }

    record WelcomeEmail(String user, int age) {
    }

    private DataSource dataSource;
    private MutableClock clock;

    @BeforeEach
    void setUp() {
        dataSource = MySqlTestSupport.freshSchema();
        clock = new MutableClock(Instant.parse("2026-08-13T00:00:00Z"), ZoneId.of("UTC"));
    }

    /**
     * Every table declares its character set, so the two statements that compare {@code job_key}
     * across tables never meet an "Illegal mix of collations" on a server whose default is not
     * {@code utf8mb4}. The five tables of the table split forgot the clause once. The assertion is
     * "the same collation as the first table", not "some utf8mb4 collation": two utf8mb4 collations
     * mix just as illegally as utf8mb4 and latin1 do.
     */
    @Test
    void everyTableSharesTheCollationOfTheFirstOne() {
        List<String> tables = new JdbcTemplate(dataSource).queryForList(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE()"
                        + " AND table_name LIKE 'mohs_%' AND table_collation <> (SELECT t.table_collation"
                        + " FROM information_schema.tables t WHERE t.table_schema = DATABASE()"
                        + " AND t.table_name = 'mohs_job_definitions')", String.class);

        assertThat(tables).as("tables whose collation differs from mohs_job_definitions").isEmpty();
    }

    @Test
    void jobStoreRoundTripsAgainstMySql() {
        JdbcJobStore store = new JdbcJobStore(dataSource, clock, new MySqlJdbcDelegate());
        JobDefinition definition = JobDefinition.of("welcome-email", Handler.class, spec -> spec.onDemand().runner("io"));

        store.upsert(definition);
        Optional<StoredJob> stored = store.find(JobKey.of("welcome-email"));

        assertThat(stored).isPresent();
        assertThat(stored.get().definition()).isEqualTo(definition);
    }

    @Test
    void historyStoreRoundTripsThePayloadStoredAsText() {
        new JdbcJobStore(dataSource, clock, new MySqlJdbcDelegate()).upsert(
                JobDefinition.of("welcome-email", Handler.class, spec -> spec.onDemand().runner("io")));
        JdbcHistoryStore store = new JdbcHistoryStore(dataSource, JsonMapper.builder().build(), new MySqlJdbcDelegate());
        Instant when = Instant.parse("2026-08-13T00:00:00Z");

        store.record(List.of(new HistoryStore.NewExecution(ExecutionId.of("019abc-1"), JobKey.of("welcome-email"),
                0, 20, when, when, "application", null, null, new WelcomeEmail("ana", 31))));
        HistoryStore.PayloadBatch batch = store.findPayloads(List.of(ExecutionId.of("019abc-1")));

        assertThat(batch.unreadable()).isEmpty();
        assertThat(batch.rows().get(ExecutionId.of("019abc-1")).payload()).isEqualTo(new WelcomeEmail("ana", 31));
    }

    /**
     * The deduplication's semantics in the real delegate, not just the DDL: the Idempotent Receiver is
     * {@code mohs_idempotency}'s primary-key conflict — the same key collides, and an execution with no
     * key never contends for the table.
     */
    @Test
    void idempotencyPrimaryKeyRejectsDuplicatesAndAllowsNullKeys() {
        new JdbcJobStore(dataSource, clock, new MySqlJdbcDelegate()).upsert(
                JobDefinition.of("welcome-email", Handler.class, spec -> spec.onDemand().runner("io")));
        JdbcHistoryStore store = new JdbcHistoryStore(dataSource, JsonMapper.builder().build(), new MySqlJdbcDelegate());

        store.record(List.of(newExecutionWithKey("idem-1", "req-1")));
        assertThatThrownBy(() -> store.record(List.of(newExecutionWithKey("idem-2", "req-1"))))
                .isInstanceOf(DuplicateKeyException.class);
        store.record(List.of(newExecutionWithKey("idem-3", null)));
        store.record(List.of(newExecutionWithKey("idem-4", null)));

        assertThat(store.findByIdempotencyKey(JobKey.of("welcome-email"), "req-1"))
                .contains(ExecutionId.of("idem-1"));
    }

    private static HistoryStore.NewExecution newExecutionWithKey(String id, @Nullable String idempotencyKey) {
        Instant when = Instant.parse("2026-08-13T00:00:00Z");
        return new HistoryStore.NewExecution(ExecutionId.of(id), JobKey.of("welcome-email"), 0, 20, when, when,
                "application", null, idempotencyKey, new WelcomeEmail("ana", 31));
    }

    /** A plain DATETIME (no fraction) would round this to the second — this proves DATETIME(6) preserves the microsecond. */
    @Test
    void historyStoreRoundTripsSubSecondPrecision() {
        new JdbcJobStore(dataSource, clock, new MySqlJdbcDelegate()).upsert(
                JobDefinition.of("welcome-email", Handler.class, spec -> spec.onDemand().runner("io")));
        JdbcHistoryStore store = new JdbcHistoryStore(dataSource, JsonMapper.builder().build(), new MySqlJdbcDelegate());
        Instant scheduledAt = Instant.parse("2026-08-13T00:00:00.123456Z");

        store.record(List.of(new HistoryStore.NewExecution(ExecutionId.of("019abc-2"), JobKey.of("welcome-email"),
                0, 20, scheduledAt, scheduledAt, "application", null, null, new WelcomeEmail("ana", 31))));

        assertThat(store.findHeads(List.of(ExecutionId.of("019abc-2"))))
                .singleElement()
                .extracting(HistoryStore.ExecutionHead::scheduledAt)
                .isEqualTo(scheduledAt);
    }

    @Test
    void batchStoreRoundTripsAgainstMySql() {
        JdbcBatchStore store = new JdbcBatchStore(dataSource, clock, new MySqlJdbcDelegate());

        store.insert("batch-1", "nightly", 10);
        BatchCounters counters = store.find("batch-1").orElseThrow();

        assertThat(counters.total()).isEqualTo(10);
    }

    @Test
    void rateLimitStoreRoundTripsAgainstMySql() {
        JdbcRateLimitStore store = new JdbcRateLimitStore(dataSource, Clock.systemUTC(), new MySqlJdbcDelegate());
        RateLimit rateLimit = new RateLimit("smtp", 100, Duration.ofMinutes(1));

        store.upsert(rateLimit);

        assertThat(store.find("smtp").map(RateLimitSnapshot::rateLimit)).contains(rateLimit);
    }

    /**
     * The token bucket lives in two columns, and {@code refilled_at} is the one carrying the fraction: the
     * refill counts tokens from the time elapsed since it and writes back the instant advanced by what it
     * converted — never "now".
     *
     * <p>If the delegate swallows that instant's sub-second fraction, the bucket wakes with the wrong age:
     * older releases an extra token and blows precisely the limit protecting the external resource;
     * younger holds a job back without a single error in the log. Until now the {@code charge} path only
     * ran on H2.
     *
     * <p>The arithmetic is chosen so as not to absorb the error. At 100/min a token comes every 600ms, the
     * row is born at {@code .500} and the second charge comes 1s later: with the fraction intact the
     * elapsed time yields exactly 1 token and 50 remain. What this test catches is a deviation of
     * {@code refilled_at} from 200ms into the past (giving 51) or beyond 400ms into the future (giving 49)
     * — a whole-second column falls outside on both sides: truncating gives 51, rounding gives 49,
     * measured by degrading MySQL to a {@code DATETIME} with no declared precision. Finer loss passes
     * here, and is harmless within any real limit's per-token interval.
     */
    @Test
    void chargingAcrossATokenBoundaryProvesRefilledAtKeepsItsFraction() {
        clock.advance(Duration.ofMillis(500));
        JdbcRateLimitStore store = new JdbcRateLimitStore(dataSource, clock, new MySqlJdbcDelegate());
        store.upsert(new RateLimit("smtp", 100, Duration.ofMinutes(1)));

        assertThat(store.charge("smtp", 50, clock.instant())).isTrue();
        clock.advance(Duration.ofSeconds(1));
        assertThat(store.charge("smtp", 1, clock.instant())).isTrue();

        assertThat(store.available("smtp", clock.instant())).isEqualTo(50);
    }

    /** The sweep's three statements in the dialect's real shape — the bare {@code DELETE ... LIMIT} form parses, ranges the primary key, and deletes exactly the window. */
    @Test
    void historySweepRoundTripsTheDialectShapes() {
        MySqlJdbcDelegate delegate = new MySqlJdbcDelegate();
        JdbcHistoryStore store = new JdbcHistoryStore(dataSource, JsonMapper.builder().build(), delegate);
        JdbcTemplate raw = new JdbcTemplate(dataSource);
        Instant old = clock.instant().minus(Duration.ofDays(60));
        long ms = old.toEpochMilli();
        String executionId = "%08x-%04x-7fff-8fff-000000000001".formatted(ms >>> 16, ms & 0xFFFF);
        String batchId = "%08x-%04x-7fff-8fff-000000000002".formatted(ms >>> 16, ms & 0xFFFF);
        raw.update("INSERT INTO mohs_batches (id, name, total, succeeded, failed, created_at) VALUES (?, 'nightly', 1, 1, 0, ?)",
                batchId, delegate.splitTimestamp(old));
        raw.update("""
                INSERT INTO mohs_execution (execution_id, job_key, shard, priority, state, scheduled_at, created_at,
                    finished_at, actor, correlation_id, payload, payload_type)
                VALUES (?, 'welcome-email', 0, 20, 'SUCCEEDED', ?, ?, ?, 'test', ?, '{}', 'x')
                """, executionId, delegate.splitTimestamp(old), delegate.splitTimestamp(old),
                delegate.splitTimestamp(old), batchId);
        raw.update("""
                INSERT INTO mohs_attempt (execution_id, number, node_id, started_at, finished_at, outcome)
                VALUES (?, 1, 'node-a', ?, ?, 'SUCCEEDED')
                """, executionId, delegate.splitTimestamp(old), delegate.splitTimestamp(old));

        assertThat(store.pruneHistoryBefore(clock.instant().minus(Duration.ofDays(30)), 100))
                .isEqualTo(new HistoryStore.PrunedHistory(1, 1, 1));
    }
}
