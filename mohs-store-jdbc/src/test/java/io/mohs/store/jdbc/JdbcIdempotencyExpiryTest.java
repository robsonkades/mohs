package io.mohs.store.jdbc;

import io.mohs.engine.SubmissionStore;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import tools.jackson.databind.json.JsonMapper;
import io.mohs.core.execution.ExecutionId;
import io.mohs.core.job.JobKey;
import io.mohs.engine.HistoryStore;
import io.mohs.store.jdbc.delegate.JdbcDelegate;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JdbcIdempotencyExpiryTest {
    private static final Instant NOW = Instant.parse("2026-09-09T12:00:00Z");
    private static final JobKey JOB = JobKey.of("expiry-job");

    @ParameterizedTest(name = "{0}")
    @MethodSource("io.mohs.store.jdbc.JdbcJobBindingTest#backends")
    void expiryIsFixedAtAcceptanceAndIndependentOfPhysicalPruning(JdbcJobBindingTest.Backend backend) {
        var time = new AtomicReference<>(NOW);
        var first = store(backend, time, Duration.ofSeconds(1));
        first.record(List.of(execution("01991337-0000-7000-8000-00000000004d")));
        var replacementSettings = store(backend, time, Duration.ofDays(30));
        time.set(NOW.plusSeconds(1).minusNanos(1000));
        assertThat(replacementSettings.findReceiptByIdempotencyKey(JOB, "key")).isPresent();
        time.set(NOW.plusSeconds(1));
        assertThat(replacementSettings.findReceiptByIdempotencyKey(JOB, "key")).isEmpty();
        assertThat(new JdbcTemplate(backend.dataSource()).queryForObject("SELECT COUNT(*) FROM mohs_idempotency", Integer.class)).isOne();
        replacementSettings.record(List.of(execution("01991337-0000-7000-8000-000000000083")));
        assertThat(replacementSettings.findReceiptByIdempotencyKey(JOB, "key").orElseThrow().executionId()).isEqualTo(ExecutionId.of("01991337-0000-7000-8000-000000000083"));
        assertThat(first.find(ExecutionId.of("01991337-0000-7000-8000-00000000004d"), time.get())).isPresent();
        assertThat(first.pruneExpiredIdempotency()).isZero();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("io.mohs.store.jdbc.JdbcJobBindingTest#backends")
    void unlimitedReceiptIsNotShortenedByNewConfiguration(JdbcJobBindingTest.Backend backend) {
        var time = new AtomicReference<>(NOW);
        store(backend, time, Duration.ZERO).record(List.of(execution("01991337-0000-7000-8000-00000000004e")));
        time.set(NOW.plus(Duration.ofDays(300)));
        var shorter = store(backend, time, Duration.ofSeconds(1));
        assertThat(shorter.pruneExpiredIdempotency()).isZero();
        assertThat(shorter.findReceiptByIdempotencyKey(JOB, "key")).isPresent();
        assertThatThrownBy(() -> shorter.record(List.of(execution("01991337-0000-7000-8000-000000000017")))).isInstanceOf(DuplicateKeyException.class);
        assertThat(shorter.find(ExecutionId.of("01991337-0000-7000-8000-000000000017"), time.get())).isEmpty();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("io.mohs.store.jdbc.JdbcJobBindingTest#backends")
    void failedReplacementRollsBackDeletionOfExpiredReceipt(JdbcJobBindingTest.Backend backend) {
        var time = new AtomicReference<>(NOW);
        var store = store(backend, time, Duration.ofSeconds(1));
        store.record(List.of(execution("01991337-0000-7000-8000-00000000007b")));
        time.set(NOW.plusSeconds(1));
        // Reusing the execution primary key fails AFTER replacement of the expired receipt.
        assertThatThrownBy(() -> store.record(List.of(execution("01991337-0000-7000-8000-00000000007b")))).isInstanceOf(DuplicateKeyException.class);
        assertThat(store.findReceiptByIdempotencyKey(JOB, "key")).isEmpty();
        assertThat(store.pruneExpiredIdempotency()).isOne();
        assertThat(store.find(ExecutionId.of("01991337-0000-7000-8000-00000000007b"), time.get())).isPresent();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("io.mohs.store.jdbc.JdbcJobBindingTest#backends")
    void twoReplacementsOfExpiredKeyHaveOneWinner(JdbcJobBindingTest.Backend backend) throws Exception {
        var time = new AtomicReference<>(NOW);
        var store = store(backend, time, Duration.ofSeconds(1));
        store.record(List.of(execution("01991337-0000-7000-8000-000000000076")));
        time.set(NOW.plusSeconds(1));
        var barrier = new CyclicBarrier(2);
        try (var threads = Executors.newVirtualThreadPerTaskExecutor()) {
            var first = threads.submit(() -> accept(store, barrier, "01991337-0000-7000-8000-000000000006"));
            var second = threads.submit(() -> accept(store, barrier, "01991337-0002-7000-8000-00000000009b"));
            assertThat(first.get(10, TimeUnit.SECONDS) + second.get(10, TimeUnit.SECONDS)).isOne();
        }
        assertThat(new JdbcTemplate(backend.dataSource()).queryForObject("SELECT COUNT(*) FROM mohs_execution", Integer.class)).isEqualTo(2);
        assertThat(store.findReceiptByIdempotencyKey(JOB, "key")).isPresent();
    }

    private static int accept(Fixture store, CyclicBarrier barrier, String id) throws Exception {
        barrier.await(5, TimeUnit.SECONDS);
        try { store.record(List.of(execution(id))); return 1; }
        catch (DuplicateKeyException conflict) { return 0; }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("io.mohs.store.jdbc.JdbcJobBindingTest#backends")
    void cleanupIsBoundedAndDoesNotTouchAnUnexpiredReceipt(JdbcJobBindingTest.Backend backend) {
        var time = new AtomicReference<>(NOW);
        var store = store(backend, time, Duration.ofDays(1));
        store.record(List.of(execution("01991337-0000-7000-8000-00000000008d")));
        var rows = java.util.stream.IntStream.range(0, 1001).mapToObj(i -> new Object[]{"expired-" + i,
                "old-" + i, JdbcTimestamps.toUtcLocalDateTime(NOW), JdbcTimestamps.toUtcLocalDateTime(NOW),
                JdbcTimestamps.toUtcLocalDateTime(NOW)}).toList();
        new JdbcTemplate(backend.dataSource()).batchUpdate("""
                INSERT INTO mohs_idempotency (job_key, idempotency_key, execution_id, created_at, expires_at, scheduled_at, actor)
                VALUES ('expiry-job', ?, ?, ?, ?, ?, 'test')
                """, rows);
        assertThat(store.pruneExpiredIdempotency()).isEqualTo(1000);
        assertThat(store.pruneExpiredIdempotency()).isOne();
        assertThat(store.pruneExpiredIdempotency()).isZero();
        assertThat(store.findReceiptByIdempotencyKey(JOB, "key")).isPresent();
    }

    private static SubmissionStore.NewExecution execution(String id) {
        return new SubmissionStore.NewExecution(ExecutionId.of(id), JOB, 0, 20, NOW, NOW, "test", null, "key", io.mohs.test.InputFixtures.encode(JOB, "input"), SubmissionStore.Origin.MANUAL, null);
    }

    /** Controls the database clock sample for exact boundary races; all transactions and SQL use the real backend. */
    private static Fixture store(JdbcJobBindingTest.Backend backend, AtomicReference<Instant> time, Duration ttl) {
        var delegate = (JdbcDelegate) Proxy.newProxyInstance(JdbcDelegate.class.getClassLoader(), new Class<?>[]{JdbcDelegate.class}, (_, method, args) -> {
            if (method.getName().equals("readNow")) return time.get();
            try { return method.invoke(backend.delegate(), args); }
            catch (InvocationTargetException failure) { throw failure.getCause(); }
        });
        TestSubmissions.declare(backend.dataSource(), delegate, List.of(execution("01991337-0000-7000-8000-000000000019")));
        return new Fixture(new JdbcHistoryStore(backend.dataSource(), delegate),
                new JdbcSubmissionStore(backend.dataSource(), delegate, ttl));
    }
    private record Fixture(JdbcHistoryStore history, JdbcSubmissionStore submissions) {
        void record(List<SubmissionStore.NewExecution> executions) { submissions.accept(executions, null, () -> {}); }
        java.util.Optional<io.mohs.core.event.Enqueued> findReceiptByIdempotencyKey(JobKey key, String id) {
            return history.findReceiptByIdempotencyKey(key, id);
        }
        java.util.Optional<io.mohs.core.execution.Execution> find(ExecutionId id, Instant now) { return history.find(id, now); }
        int pruneExpiredIdempotency() { return history.pruneExpiredIdempotency(); }
    }
}
