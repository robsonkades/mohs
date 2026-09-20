package io.mohs.store.jdbc;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import io.mohs.core.definition.JobDefinition;
import io.mohs.core.definition.JobOptions;
import io.mohs.core.job.JobKey;
import io.mohs.core.schedule.IntervalSpec;
import io.mohs.core.schedule.Misfire;
import io.mohs.engine.CatalogStore;
import io.mohs.engine.ExecutableCatalog;
import io.mohs.engine.JobBinding;
import io.mohs.store.jdbc.delegate.MySqlJdbcDelegate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Locking decisions must use current rows even when the host already established an older RR snapshot. */
@Tag("docker")
class JdbcJobSnapshotMySqlTest {
    private static final Instant NOW = Instant.parse("2026-09-19T12:00:00Z");
    private static final JobKey KEY = JobKey.of("snapshot-job");
    private static final IntervalSpec ORIGINAL = new IntervalSpec(Duration.ofMinutes(1), false);
    private static final IntervalSpec CHANGED = new IntervalSpec(Duration.ofMinutes(5), false);

    private DataSource source;
    private JdbcJobStore jobs;
    private TransactionTemplate host;

    @BeforeEach
    void setUp() {
        source = MySqlTestSupport.freshSchema();
        jobs = new JdbcJobStore(source, Clock.fixed(NOW, ZoneOffset.UTC), new MySqlJdbcDelegate());
        host = new TransactionTemplate(new DataSourceTransactionManager(source));
        host.setIsolationLevel(TransactionDefinition.ISOLATION_REPEATABLE_READ);
        host.setTimeout(10);
        jobs.upsert(definition());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void reconfigurationUsesTheLockedScheduleInsteadOfTheHostsSnapshot(boolean bound) {
        host.executeWithoutResult(_ -> {
            assertThat(jobs.find(KEY).orElseThrow().definition().schedule()).isEqualTo(ORIGINAL);
            independently(() -> assertThat(jobs.reschedule(KEY, CHANGED)).isTrue());

            if (bound) jobs.upsertBound(definition(), new JobBinding(KEY, KEY, null));
            else jobs.upsert(definition());

            var updated = jobs.find(KEY).orElseThrow();
            assertThat(updated.definition().schedule()).isEqualTo(ORIGINAL);
            assertThat(updated.nextFireAt()).isEqualTo(NOW.plus(ORIGINAL.interval()));
            assertThat(updated.scheduleRevision()).isEqualTo(3);
        });
    }

    @Test
    void publicationStillRollsBackWithTheHostTransaction() {
        host.executeWithoutResult(scope -> {
            jobs.find(KEY).orElseThrow();
            independently(() -> jobs.reschedule(KEY, CHANGED));
            jobs.upsert(definition());
            scope.setRollbackOnly();
        });

        var committed = jobs.find(KEY).orElseThrow();
        assertThat(committed.definition().schedule()).isEqualTo(CHANGED);
        assertThat(committed.nextFireAt()).isEqualTo(NOW.plus(CHANGED.interval()));
        assertThat(committed.scheduleRevision()).isEqualTo(2);
    }

    @Test
    void reactivationUsesRetirementCommittedAfterTheHostSnapshot() {
        var catalog = new ExecutableCatalog(Map.of(KEY, KEY.value()));
        host.executeWithoutResult(_ -> {
            jobs.find(KEY).orElseThrow();
            independently(() -> jobs.remove(KEY));
            assertThat(jobs.reactivate(KEY, catalog)).isTrue();
        });

        assertThat(jobs.find(KEY).orElseThrow().scheduleRevision()).isEqualTo(3);
    }

    @Test
    void reactivationCannotJoinAnOldCatalogHiddenByTheHostSnapshot() {
        var originalCatalog = new ExecutableCatalog(Map.of(KEY, KEY.value()));
        var catalogs = new JdbcCatalogStore(source, Clock.fixed(NOW, ZoneOffset.UTC), new MySqlJdbcDelegate());
        catalogs.reconcile(originalCatalog, List.of(definition()), CatalogStore.Conflict.OVERRIDE);
        assertThatThrownBy(() -> host.executeWithoutResult(_ -> {
            new JdbcTemplate(source).queryForObject("SELECT fingerprint FROM mohs_catalog", String.class);
            independently(() -> {
                jobs.remove(KEY);
                catalogs.reconcile(new ExecutableCatalog(Map.of()), List.of(), CatalogStore.Conflict.OVERRIDE);
            });
            jobs.reactivate(KEY, originalCatalog);
        })).isInstanceOf(IllegalStateException.class).hasMessageContaining("differs");
        assertThat(jobs.find(KEY)).isEmpty();
    }

    private static JobDefinition definition() {
        return JobDefinition.of(KEY.value(), ORIGINAL, JobOptions.defaults(), Misfire.IGNORE, false);
    }

    private static void independently(Runnable operation) {
        try (var threads = Executors.newVirtualThreadPerTaskExecutor()) {
            threads.submit(operation).get(5, TimeUnit.SECONDS);
        } catch (Exception failure) {
            throw new AssertionError("concurrent committed change failed", failure);
        }
    }
}
