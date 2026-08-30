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

import tools.jackson.databind.json.JsonMapper;

import io.mohs.core.definition.JobDefinition;
import io.mohs.core.execution.ExecutionId;
import io.mohs.core.job.JobKey;
import io.mohs.core.RateLimitSnapshot;
import io.mohs.core.resource.RateLimit;
import io.mohs.engine.BatchCounters;
import io.mohs.engine.HistoryStore;
import io.mohs.engine.StoredJob;
import io.mohs.store.jdbc.dialect.PostgresJdbcDialect;
import io.mohs.test.MutableClock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Proves that the schema (all of it, not only what {@code JdbcWorkQueue} touches) and each store's DML
 * round-trip against a real Postgres, not just H2 — in particular
 * {@code mohs_execution.payload}/{@code mohs_attempt.error} ({@code TEXT}, formerly {@code CLOB}), the
 * one real divergence confirmed between the two databases. One round trip per store, not each one's
 * whole suite — the other portability findings are SQL Server-specific, outside this round's scope.
 */
class SchemaPostgresRoundTripTest {

    record Handler() {
    }

    record WelcomeEmail(String user, int age) {
    }

    private DataSource dataSource;
    private MutableClock clock;

    @BeforeEach
    void setUp() {
        dataSource = PostgresTestSupport.freshSchema();
        clock = new MutableClock(Instant.parse("2026-08-13T00:00:00Z"), ZoneId.of("UTC"));
    }

    @Test
    void jobStoreRoundTripsAgainstPostgres() {
        JdbcJobStore store = new JdbcJobStore(dataSource, clock);
        JobDefinition definition = JobDefinition.of("welcome-email", Handler.class, spec -> spec.onDemand().runner("io"));

        store.upsert(definition);
        Optional<StoredJob> stored = store.find(JobKey.of("welcome-email"));

        assertThat(stored).isPresent();
        assertThat(stored.get().definition()).isEqualTo(definition);
    }

    @Test
    void historyStoreRoundTripsThePayloadStoredAsText() {
        new JdbcJobStore(dataSource, clock).upsert(
                JobDefinition.of("welcome-email", Handler.class, spec -> spec.onDemand().runner("io")));
        JdbcHistoryStore store = new JdbcHistoryStore(dataSource, JsonMapper.builder().build(), new PostgresJdbcDialect());
        Instant when = Instant.parse("2026-08-13T00:00:00Z");

        store.record(List.of(new HistoryStore.NewExecution(ExecutionId.of("019abc-1"), JobKey.of("welcome-email"),
                0, 20, when, when, "application", null, null, new WelcomeEmail("ana", 31))));
        HistoryStore.PayloadBatch batch = store.findPayloads(List.of(ExecutionId.of("019abc-1")));

        assertThat(batch.unreadable()).isEmpty();
        assertThat(batch.rows().get(ExecutionId.of("019abc-1")).payload()).isEqualTo(new WelcomeEmail("ana", 31));
    }

    /**
     * The deduplication's semantics in the real dialect, not just the DDL: the Idempotent Receiver is
     * {@code mohs_idempotency}'s primary-key conflict — the same key collides, and an execution with no
     * key never contends for the table.
     */
    @Test
    void idempotencyPrimaryKeyRejectsDuplicatesAndAllowsNullKeys() {
        new JdbcJobStore(dataSource, clock).upsert(
                JobDefinition.of("welcome-email", Handler.class, spec -> spec.onDemand().runner("io")));
        JdbcHistoryStore store = new JdbcHistoryStore(dataSource, JsonMapper.builder().build(), new PostgresJdbcDialect());

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

    @Test
    void batchStoreRoundTripsAgainstPostgres() {
        JdbcBatchStore store = new JdbcBatchStore(dataSource, clock);

        store.insert("batch-1", "nightly", 10);
        BatchCounters counters = store.find("batch-1").orElseThrow();

        assertThat(counters.total()).isEqualTo(10);
    }

    @Test
    void rateLimitStoreRoundTripsAgainstPostgres() {
        JdbcRateLimitStore store = new JdbcRateLimitStore(dataSource, Clock.systemUTC());
        RateLimit rateLimit = new RateLimit("smtp", 100, Duration.ofMinutes(1));

        store.upsert(rateLimit);

        assertThat(store.find("smtp").map(RateLimitSnapshot::rateLimit)).contains(rateLimit);
    }

    /**
     * The token bucket lives in two columns, and {@code refilled_at} is the one carrying the fraction: the
     * refill counts tokens from the time elapsed since it and writes back the instant advanced by what it
     * converted — never "now".
     *
     * <p>If the dialect swallows that instant's sub-second fraction, the bucket wakes with the wrong age:
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
        JdbcRateLimitStore store = new JdbcRateLimitStore(dataSource, clock);
        store.upsert(new RateLimit("smtp", 100, Duration.ofMinutes(1)));

        assertThat(store.charge("smtp", 50, clock.instant())).isTrue();
        clock.advance(Duration.ofSeconds(1));
        assertThat(store.charge("smtp", 1, clock.instant())).isTrue();

        assertThat(store.available("smtp", clock.instant())).isEqualTo(50);
    }
}
