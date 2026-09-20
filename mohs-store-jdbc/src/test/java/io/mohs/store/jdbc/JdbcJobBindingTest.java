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

import io.mohs.engine.SubmissionStore;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import javax.sql.DataSource;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.json.JsonMapper;
import io.github.robsonkades.uuidv7.UUIDv7;
import io.mohs.core.definition.JobOptions;
import io.mohs.core.definition.JobDefinition;
import io.mohs.core.definition.DefinitionSource;
import io.mohs.core.execution.Execution;
import io.mohs.core.execution.ExecutionId;
import io.mohs.core.execution.Priority;
import io.mohs.core.job.JobKey;
import io.mohs.core.schedule.IntervalSpec;
import io.mohs.core.schedule.Misfire;
import io.mohs.core.schedule.OnDemandSpec;
import io.mohs.engine.JobBinding;
import io.mohs.engine.JobDefinitions;
import io.mohs.engine.TriggerFirer;
import io.mohs.store.jdbc.delegate.*;
import io.mohs.test.MutableClock;

import static org.assertj.core.api.Assertions.*;

class JdbcJobBindingTest {
    private static final Instant NOW = Instant.parse("2026-09-08T12:00:00Z");
    public record Input(String company, int year) {}
    record Backend(String name, DataSource dataSource, JdbcDelegate delegate) {
        @Override public String toString() { return name; }
    }

    static Stream<Backend> backends() {
        JdbcDataSource h2 = new JdbcDataSource();
        h2.setURL("jdbc:h2:mem:bindings-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        new ResourceDatabasePopulator(new ClassPathResource("schema-h2.sql")).execute(h2);
        return Stream.of(new Backend("h2", h2, new H2JdbcDelegate()),
                new Backend("postgresql", PostgresTestSupport.freshSchema(), new PostgresJdbcDelegate()),
                new Backend("mysql", MySqlTestSupport.freshSchema(), new MySqlJdbcDelegate()),
                new Backend("sqlserver", SqlServerTestSupport.freshSchema(), new SqlServerJdbcDelegate()));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("backends")
    void indexedDependencyChecksPreserveRetainedIdentityAndIgnoreRetiredDependents(Backend backend) throws Exception {
        var store = new JdbcJobStore(backend.dataSource(), MutableClock.startingAt(NOW), backend.delegate());
        var history = new JdbcHistoryStore(backend.dataSource(), backend.delegate());
        JobKey owner = JobKey.of("owner-" + UUID.randomUUID());
        JobKey schedule = JobKey.of("dependent-" + UUID.randomUUID());
        store.upsertBound(JobDefinitions.definition(owner, JobOptions.defaults(), new OnDemandSpec(), Misfire.IGNORE, false),
                new JobBinding(owner, owner, null));
        assertThat(store.findExecutableKey(owner)).contains(owner);
        assertThat(store.findExecutableKey(schedule)).isEmpty();
        assertThat(store.hasActiveDependents(owner)).isFalse();
        store.upsertBound(JobDefinitions.definition(schedule, JobOptions.defaults(),
                        new IntervalSpec(Duration.ofHours(1), false), Misfire.IGNORE, true),
                new JobBinding(schedule, owner, io.mohs.test.InputFixtures.encode(owner, new Input("A", 2026))));
        // Paused schedules still depend on their executable.
        assertThat(store.hasActiveDependents(owner)).isTrue();
        assertThat(store.hasActiveDependents(schedule)).isFalse();
        assertThatThrownBy(() -> store.remove(owner)).isInstanceOf(IllegalArgumentException.class);
        store.remove(schedule);
        assertThat(store.hasActiveDependents(owner)).isFalse();
        store.remove(owner);
        assertThat(store.findExecutableKey(schedule)).contains(owner);
        assertThat(store.findExecutableKey(owner)).contains(owner);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("backends")
    void annotatedExecutableUsesTheSameBindingAndDependentRetirementRules(Backend backend) {
        var store = new JdbcJobStore(backend.dataSource(), MutableClock.startingAt(NOW), backend.delegate());
        JobKey owner = JobKey.of("annotated-owner");
        var definition = new JobDefinition(owner, null,owner, new OnDemandSpec(), null, null, null,
                Misfire.IGNORE, false, 0, 0, null, null, DefinitionSource.ANNOTATION, owner.value());
        store.upsert(definition);
        assertThat(store.findBinding(owner)).contains(new JobBinding(owner, owner, null));
        JobKey key = JobKey.of("annotated-dependent");
        store.upsertBound(JobDefinitions.definition(key, JobOptions.defaults(),
                        new IntervalSpec(Duration.ofHours(1), false), Misfire.IGNORE, false),
                new JobBinding(key, owner, new io.mohs.engine.JobPayload("{\"company\":\"A\",\"year\":2026}", owner.value())));
        assertThatThrownBy(() -> store.remove(owner)).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("active recurring");
        assertThat(store.find(owner)).isPresent();
        assertThat(store.find(key)).isPresent();
        store.remove(key);
        store.remove(owner);
        assertThat(store.find(owner)).isEmpty();
        assertThat(store.findBinding(key).orElseThrow().executableKey()).isEqualTo(owner);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("backends")
    void encodedHistoryReusesVerifiedSnapshotForEveryOccurrence(Backend backend) {
        var history = new JdbcHistoryStore(backend.dataSource(), backend.delegate());
        var encoded = new io.mohs.engine.JobPayload("{\"company\":\"A\", \"year\":2026}", "encoded-input-v1");

        JobKey key = JobKey.of("encoded-" + UUID.randomUUID());
        var executions = java.util.stream.IntStream.range(0, 3).mapToObj(i ->
                new io.mohs.engine.SubmissionStore.NewExecution(ExecutionId.of(UUIDv7.randomUUIDString()), key, 0,
                        Priority.NORMAL.value(), NOW.plusSeconds(i), NOW, Execution.SCHEDULER_ACTOR, null, null, encoded, io.mohs.engine.SubmissionStore.Origin.MANUAL, null)).toList();
        new TransactionTemplate(new DataSourceTransactionManager(backend.dataSource())).executeWithoutResult(_ ->
                TestSubmissions.accept(backend.dataSource(), backend.delegate(), executions));
        var jdbc = new JdbcTemplate(backend.dataSource());
        for (var execution : executions) {
            assertThat(jdbc.queryForObject("SELECT payload FROM mohs_execution WHERE execution_id = ?",
                    String.class, execution.executionId().value())).isEqualTo(encoded.json());
            assertThat(history.findPayloads(List.of(execution.executionId())).rows().get(execution.executionId()).payload())
                    .isEqualTo(encoded);
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("backends")
    void receiptRetainsTheAcceptanceSnapshotAfterHistoryIsPruned(Backend backend) {
        var history = new JdbcHistoryStore(backend.dataSource(), backend.delegate());
        var jdbc = new JdbcTemplate(backend.dataSource());
        JobKey job = JobKey.of("receipt-job");
        ExecutionId id = ExecutionId.of(UUIDv7.randomUUIDString());
        Instant scheduledAt = NOW.plusNanos(999_999_999);
        TestSubmissions.accept(backend.dataSource(), backend.delegate(), List.of(new io.mohs.engine.SubmissionStore.NewExecution(
                id, job, 0, Priority.NORMAL.value(), scheduledAt, NOW, "application", null, "request-key", io.mohs.test.InputFixtures.encode(job, new Input("A", 2026)), io.mohs.engine.SubmissionStore.Origin.MANUAL, null)));
        var receipt = history.findReceiptByIdempotencyKey(job, "request-key").orElseThrow();
        assertThat(receipt.scheduledAt()).isEqualTo(io.mohs.core.TimePrecision.eligible(scheduledAt));
        jdbc.update("DELETE FROM mohs_execution WHERE execution_id = ?", id.value());
        assertThat(history.findReceiptByIdempotencyKey(job, "request-key")).contains(receipt);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("backends")
    void concurrentRegistrationAndRecurrenceUpsertsPreserveOneIdentity(Backend backend) throws Exception {
        var clock = MutableClock.startingAt(NOW);
        var store = new JdbcJobStore(backend.dataSource(), clock, backend.delegate());
        var history = new JdbcHistoryStore(backend.dataSource(), backend.delegate());
        JobKey executable = JobKey.of("tax-" + UUID.randomUUID());
        var definition = JobDefinitions.definition(executable, JobOptions.defaults(),
                new OnDemandSpec(), Misfire.IGNORE, false);
        CountDownLatch start = new CountDownLatch(1);
        Runnable register = () -> {
            waitFor(start);
            store.upsertBound(definition, new JobBinding(executable, executable, null));
        };
        var a = CompletableFuture.runAsync(register);
        var b = CompletableFuture.runAsync(register);
        start.countDown();
        CompletableFuture.allOf(a, b).get(20, TimeUnit.SECONDS);
        JobKey schedule = JobKey.of("schedule-" + UUID.randomUUID());
        var recurring = JobDefinitions.definition(schedule, JobOptions.defaults().withMaxConcurrentExecutions(1),
                new IntervalSpec(Duration.ofHours(1), false), Misfire.FIRE_NOW, true);
        var input = io.mohs.test.InputFixtures.encode(executable, new Input("A", 2026));
        var first = store.upsertBound(recurring, new JobBinding(schedule, executable, input));
        store.resume(schedule);
        var updated = store.upsertBound(recurring, new JobBinding(schedule, executable, io.mohs.test.InputFixtures.encode(executable, new Input("B", 2027))));
        assertThat(updated.nextFireAt()).isEqualTo(first.nextFireAt());
        assertThat(updated.paused()).isFalse();
        assertThat(io.mohs.test.InputFixtures.decode(store.findBinding(schedule).orElseThrow().input(), Input.class)).isEqualTo(new Input("B", 2027));

        JobKey other = JobKey.of("other-" + UUID.randomUUID());
        store.upsertBound(JobDefinitions.definition(other, JobOptions.defaults(), new OnDemandSpec(), Misfire.IGNORE, false)
                        .withInputContract(executable.value()),
                new JobBinding(other, other, null));
        new TransactionTemplate(new DataSourceTransactionManager(backend.dataSource())).executeWithoutResult(status -> {
            assertThatThrownBy(() -> store.upsertBound(recurring, new JobBinding(schedule, other, input)))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("another");
            assertThat(store.findBinding(schedule).orElseThrow().executableKey()).isEqualTo(executable);
        });
        assertThatThrownBy(() -> store.remove(executable)).isInstanceOf(IllegalArgumentException.class);
        store.remove(schedule);
        store.remove(executable);
        assertThat(store.find(executable)).isEmpty();
        assertThat(store.findBinding(schedule)).isPresent();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("backends")
    void firingReadsTheCommittedInputUnderItsTriggerLock(Backend backend) throws Exception {
        var clock = MutableClock.startingAt(NOW);
        var store = new JdbcJobStore(backend.dataSource(), clock, backend.delegate());
        var history = new JdbcHistoryStore(backend.dataSource(), backend.delegate());
        var batches = new JdbcBatchStore(backend.dataSource(), backend.delegate());
        var queue = new JdbcWorkQueue(backend.dataSource(), backend.delegate());
        var firer = new JdbcSubmissionStore(backend.dataSource(), backend.delegate(), java.time.Duration.ofDays(7));
        JobKey executable = JobKey.of("tax-" + UUID.randomUUID());
        store.upsertBound(JobDefinitions.definition(executable, JobOptions.defaults(), new OnDemandSpec(), Misfire.IGNORE, false),
                new JobBinding(executable, executable, null));
        JobKey schedule = JobKey.of("schedule-" + UUID.randomUUID());
        var definition = JobDefinitions.definition(schedule, JobOptions.defaults(),
                new IntervalSpec(Duration.ofHours(1), false), Misfire.IGNORE, false);
        var initial = store.upsertBound(definition,
                new JobBinding(schedule, executable, io.mohs.test.InputFixtures.encode(executable, new Input("A", 2026))));
        Instant due = initial.nextFireAt();
        ExecutionId id = ExecutionId.of(UUIDv7.randomUUIDString());
        var occurrence = Execution.enqueued(id, schedule, due, Execution.SCHEDULER_ACTOR, Priority.NORMAL);
        CountDownLatch updated = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        var writer = CompletableFuture.runAsync(() ->
                new TransactionTemplate(new DataSourceTransactionManager(backend.dataSource())).executeWithoutResult(status -> {
                    store.upsertBound(definition, new JobBinding(schedule, executable, io.mohs.test.InputFixtures.encode(executable, new Input("B", 2027))));
                    updated.countDown();
                    waitFor(release);
                }));
        try {
            assertThat(updated.await(10, TimeUnit.SECONDS)).isTrue();
            var firing = CompletableFuture.supplyAsync(() ->
                    firer.fire(schedule, 1L, due, due.plusSeconds(3600), List.of(occurrence), due));
            release.countDown();
            writer.get(10, TimeUnit.SECONDS);
            assertThat(firing.get(10, TimeUnit.SECONDS)).isTrue();
            String persisted = new JdbcTemplate(backend.dataSource()).queryForObject(
                    "SELECT payload FROM mohs_execution WHERE execution_id = ?", String.class, id.value());
            assertThat(persisted).contains("2027").contains("B");
            store.upsertBound(definition, new JobBinding(schedule, executable, io.mohs.test.InputFixtures.encode(executable, new Input("C", 2028))));
            assertThat(new JdbcTemplate(backend.dataSource()).queryForObject(
                    "SELECT payload FROM mohs_execution WHERE execution_id = ?", String.class, id.value())).isEqualTo(persisted);
        } finally {
            release.countDown();
        }
    }

    private static void waitFor(CountDownLatch latch) {
        try {
            if (!latch.await(15, TimeUnit.SECONDS)) throw new IllegalStateException("test coordination timed out");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
