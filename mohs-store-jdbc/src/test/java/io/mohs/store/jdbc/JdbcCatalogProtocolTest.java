package io.mohs.store.jdbc;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.jdbc.core.JdbcTemplate;
import io.mohs.core.EngineState;
import io.mohs.core.definition.JobDefinition;
import io.mohs.core.definition.JobOptions;
import io.mohs.core.job.JobKey;
import io.mohs.core.schedule.Misfire;
import io.mohs.core.schedule.OnDemandSpec;
import io.mohs.engine.CatalogStore;
import io.mohs.engine.ExecutableCatalog;
import static org.assertj.core.api.Assertions.*;

class JdbcCatalogProtocolTest {
    @ParameterizedTest(name = "{0}")
    @MethodSource("io.mohs.store.jdbc.JdbcJobBindingTest#backends")
    void publicationWaitsForTheSessionRenewalAndChecksItsNewExpiry(JdbcJobBindingTest.Backend backend) throws Exception {
        var time = new java.util.concurrent.atomic.AtomicReference<>(java.time.Instant.parse("2026-09-09T12:00:00Z"));
        var renewAtCommit = new java.util.concurrent.CountDownLatch(1);
        var allowRenewCommit = new java.util.concurrent.CountDownLatch(1);
        var publicationAtSession = new java.util.concurrent.CountDownLatch(1);
        var timed = timedDelegate(backend.delegate(), time, null);
        var nodes = new JdbcNodeStore(backend.dataSource(), timed);
        var empty = new ExecutableCatalog(Map.of());
        String node = io.mohs.test.TestIds.value("catalog-renew-race");
        nodes.register(node, EngineState.RUNNING, Duration.ofSeconds(1), empty);
        time.set(time.get().plusMillis(500));
        var blocked = new JdbcReadModelAtomicityTest.BeforeCommit(backend.dataSource(), () -> {
            renewAtCommit.countDown();
            await(allowRenewCommit);
        });
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var renewal = executor.submit(() -> new JdbcNodeStore(blocked, timed).renew(node, EngineState.RUNNING, TTL));
            try {
                assertThat(renewAtCommit.await(5, TimeUnit.SECONDS)).isTrue();
                time.set(time.get().plusSeconds(2)); // Original expiry is now past; renewed expiry remains future.
                var publication = executor.submit(() -> {
                    var catalogs = new JdbcCatalogStore(backend.dataSource(), Clock.systemUTC(),
                            timedDelegate(backend.delegate(), time, publicationAtSession));
                    assertThatThrownBy(() -> catalogs.reconcile(catalog("new-job"), List.of(definition("new-job")), CatalogStore.Conflict.OVERRIDE))
                            .isInstanceOf(IllegalStateException.class).hasMessageContaining("stop or expire");
                });
                assertThat(publicationAtSession.await(5, TimeUnit.SECONDS)).isTrue();
                allowRenewCommit.countDown();
                assertThat(renewal.get(5, TimeUnit.SECONDS)).isPresent();
                publication.get(5, TimeUnit.SECONDS);
            } finally {
                allowRenewCommit.countDown();
            }
        }
        assertThat(new JdbcTemplate(backend.dataSource()).queryForObject("SELECT fingerprint FROM mohs_catalog", String.class))
                .isEqualTo(empty.fingerprint());
    }

    private static io.mohs.store.jdbc.delegate.JdbcDelegate timedDelegate(io.mohs.store.jdbc.delegate.JdbcDelegate delegate,
            java.util.concurrent.atomic.AtomicReference<java.time.Instant> time,
            java.util.concurrent.@org.jspecify.annotations.Nullable CountDownLatch atLock) {
        return (io.mohs.store.jdbc.delegate.JdbcDelegate) java.lang.reflect.Proxy.newProxyInstance(
                delegate.getClass().getClassLoader(), new Class<?>[]{io.mohs.store.jdbc.delegate.JdbcDelegate.class}, (_, method, args) -> {
                    if (method.getName().equals("readNow")) return time.get();
                    if (method.getName().equals("lockNode") && atLock != null) atLock.countDown();
                    try { return method.invoke(delegate, args); }
                    catch (java.lang.reflect.InvocationTargetException failure) { throw failure.getCause(); }
                });
    }

    private static void await(java.util.concurrent.CountDownLatch latch) {
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) throw new IllegalStateException("test renewal barrier timed out");
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(failure);
        }
    }
    private static final Duration TTL = Duration.ofMinutes(5);
    private static ExecutableCatalog catalog(String key) {
        return new ExecutableCatalog(Map.of(JobKey.of(key), key));
    }
    private static JobDefinition definition(String key) {
        return JobDefinition.of(key, new OnDemandSpec(), JobOptions.defaults(), Misfire.IGNORE, false);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("io.mohs.store.jdbc.JdbcJobBindingTest#backends")
    void reactivationIsExplicitAndRequiresTheInstalledCatalog(JdbcJobBindingTest.Backend backend) {
        var catalogs = new JdbcCatalogStore(backend.dataSource(), Clock.systemUTC(), backend.delegate());
        var jobs = new JdbcJobStore(backend.dataSource(), Clock.systemUTC(), backend.delegate());
        var incoming = definition("a");
        catalogs.reconcile(catalog("a"), List.of(incoming), CatalogStore.Conflict.OVERRIDE);
        jobs.pause(incoming.key());
        long revision = jobs.find(incoming.key()).orElseThrow().scheduleRevision();
        jobs.remove(incoming.key());
        assertThatThrownBy(() -> jobs.upsert(incoming)).hasMessageContaining("use reactivate");
        catalogs.reconcile(catalog("a"), List.of(incoming), CatalogStore.Conflict.OVERRIDE);
        assertThat(jobs.find(incoming.key())).isEmpty();
        assertThat(jobs.reactivate(incoming.key(), catalog("a"))).isTrue();
        assertThat(jobs.reactivate(incoming.key(), catalog("a"))).isFalse();
        assertThat(jobs.find(incoming.key()).orElseThrow().paused()).isTrue();
        assertThat(jobs.find(incoming.key()).orElseThrow().scheduleRevision()).isEqualTo(revision + 2);

        jobs.remove(incoming.key());
        catalogs.reconcile(new ExecutableCatalog(Map.of()), List.of(), CatalogStore.Conflict.OVERRIDE);
        assertThatThrownBy(() -> jobs.reactivate(incoming.key(), catalog("a"))).hasMessageContaining("differs");
        assertThat(jobs.find(incoming.key())).isEmpty();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("io.mohs.store.jdbc.JdbcJobBindingTest#backends")
    void incompatibleFirstJoinsCannotBothCommitWithAnInitiallyEmptyNodeTable(JdbcJobBindingTest.Backend backend) throws Exception {
        var nodes = new JdbcNodeStore(backend.dataSource(), backend.delegate());
        var barrier = new CyclicBarrier(2);
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var attempts = List.of("catalog-a", "catalog-b").stream().map(key -> executor.submit(() -> {
                barrier.await(5, TimeUnit.SECONDS);
                try {
                    nodes.register(io.mohs.test.TestIds.value(key), EngineState.RUNNING, TTL, catalog(key));
                    return true;
                } catch (IllegalStateException expected) {
                    assertThat(expected).hasMessageContaining("catalog");
                    return false;
                }
            })).toList();
            int accepted = 0;
            for (var attempt : attempts) if (attempt.get(10, TimeUnit.SECONDS)) accepted++;
            assertThat(accepted).isEqualTo(1);
            assertThat(nodes.findAll()).hasSize(1);
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("io.mohs.store.jdbc.JdbcJobBindingTest#backends")
    void changingTheCatalogCannotPublishUnknownJobsWhileAnOldWorkerIsLive(JdbcJobBindingTest.Backend backend) {
        var catalogs = new JdbcCatalogStore(backend.dataSource(), Clock.systemUTC(), backend.delegate());
        var nodes = new JdbcNodeStore(backend.dataSource(), backend.delegate());
        catalogs.reconcile(catalog("a"), List.of(definition("a")), CatalogStore.Conflict.OVERRIDE);
        nodes.register("01991337-0000-7000-8000-00000000007a", EngineState.PAUSED, TTL, catalog("a"));
        var expanded = new ExecutableCatalog(Map.of(JobKey.of("a"), "a", JobKey.of("b"), "b"));
        assertThatThrownBy(() -> catalogs.reconcile(expanded, List.of(definition("a"), definition("b")), CatalogStore.Conflict.OVERRIDE))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("stop or expire");
        var jobs = new JdbcJobStore(backend.dataSource(), Clock.systemUTC(), backend.delegate());
        assertThat(jobs.find(JobKey.of("b"))).isEmpty();
        assertThatThrownBy(() -> jobs.upsert(definition("b"))).hasMessageContaining("complete catalog reconciliation");
        assertThat(nodes.renew("01991337-0000-7000-8000-00000000007a", EngineState.RUNNING, TTL)).isPresent();
        nodes.stop("01991337-0000-7000-8000-00000000007a");
        catalogs.reconcile(expanded, List.of(definition("a"), definition("b")), CatalogStore.Conflict.OVERRIDE);
        assertThatThrownBy(() -> nodes.register("01991337-0000-7000-8000-000000000077", EngineState.RUNNING, TTL, catalog("a")))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("differs");
        nodes.register("01991337-0000-7000-8000-000000000061", EngineState.RUNNING, TTL, expanded);
        assertThat(jobs.find(JobKey.of("b"))).isPresent();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("io.mohs.store.jdbc.JdbcJobBindingTest#backends")
    void missingPersistedExecutableRollsBackTheWholeCatalogPublication(JdbcJobBindingTest.Backend backend) {
        var catalogs = new JdbcCatalogStore(backend.dataSource(), Clock.systemUTC(), backend.delegate());
        catalogs.reconcile(catalog("a"), List.of(definition("a")), CatalogStore.Conflict.OVERRIDE);
        assertThatThrownBy(() -> catalogs.reconcile(catalog("b"), List.of(definition("b")), CatalogStore.Conflict.OVERRIDE))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("requires executable 'a'");
        var jobs = new JdbcJobStore(backend.dataSource(), Clock.systemUTC(), backend.delegate());
        assertThat(jobs.find(JobKey.of("b"))).isEmpty();
        assertThat(new JdbcTemplate(backend.dataSource()).queryForObject("SELECT fingerprint FROM mohs_catalog", String.class))
                .isEqualTo(catalog("a").fingerprint());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("io.mohs.store.jdbc.JdbcJobBindingTest#backends")
    void reconcileAndFirstJoinSerializeBeforeEitherCanPublishWork(JdbcJobBindingTest.Backend backend) throws Exception {
        var nodes = new JdbcNodeStore(backend.dataSource(), backend.delegate());
        var catalogs = new JdbcCatalogStore(backend.dataSource(), Clock.systemUTC(), backend.delegate());
        var barrier = new CyclicBarrier(2);
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var join = executor.submit(() -> {
                barrier.await(5, TimeUnit.SECONDS);
                try { nodes.register("01991337-0000-7000-8000-000000000076", EngineState.RUNNING, TTL, new ExecutableCatalog(Map.of())); return true; }
                catch (IllegalStateException expected) { return false; }
            });
            var publication = executor.submit(() -> {
                barrier.await(5, TimeUnit.SECONDS);
                try { catalogs.reconcile(catalog("new-job"), List.of(definition("new-job")), CatalogStore.Conflict.OVERRIDE); return true; }
                catch (IllegalStateException expected) { return false; }
            });
            assertThat(join.get(10, TimeUnit.SECONDS) ^ publication.get(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("io.mohs.store.jdbc.JdbcJobBindingTest#backends")
    void aRetiredExecutablesPendingGrantStillRequiresItsContract(JdbcJobBindingTest.Backend backend) {
        var catalogs = new JdbcCatalogStore(backend.dataSource(), Clock.systemUTC(), backend.delegate());
        catalogs.reconcile(catalog("a"), List.of(definition("a")), CatalogStore.Conflict.OVERRIDE);
        var jdbc = new JdbcTemplate(backend.dataSource());
        var now = JdbcTimestamps.toUtcLocalDateTime(java.time.Instant.now());
        jdbc.update("UPDATE mohs_job_definitions SET retired = ? WHERE job_key = 'a'", true);
        jdbc.update("""
                INSERT INTO mohs_execution (execution_id, job_key, state, scheduled_at, created_at, actor, payload, input_contract)
                VALUES ('pending-retired', 'a', 'PENDING', ?, ?, 'test', '{}', 'a')
                """, now, now);
        assertThatThrownBy(() -> catalogs.reconcile(new ExecutableCatalog(Map.of()), List.of(), CatalogStore.Conflict.OVERRIDE))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("requires executable 'a'");
        jdbc.update("UPDATE mohs_execution SET state = 'FAILED', finished_at = ?", now);
        catalogs.reconcile(new ExecutableCatalog(Map.of()), List.of(), CatalogStore.Conflict.OVERRIDE);
    }
}
