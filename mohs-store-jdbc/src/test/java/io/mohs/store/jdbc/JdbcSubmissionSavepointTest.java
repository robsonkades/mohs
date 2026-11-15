package io.mohs.store.jdbc;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import io.mohs.core.definition.JobDefinition;
import io.mohs.core.definition.JobOptions;
import io.mohs.core.execution.ExecutionId;
import io.mohs.core.job.JobKey;
import io.mohs.core.schedule.Misfire;
import io.mohs.core.schedule.OnDemandSpec;
import io.mohs.engine.JobPayload;
import io.mohs.engine.SubmissionStore;
import io.mohs.store.jdbc.delegate.H2JdbcDelegate;
import io.mohs.test.TestIds;

import static org.assertj.core.api.Assertions.assertThat;

/** Real savepoint rollback must discard only callbacks whose accepted writes were rolled back. */
class JdbcSubmissionSavepointTest {
    private static final Instant NOW = Instant.parse("2026-09-19T12:00:00Z");
    private static final JobKey JOB = JobKey.of("savepoint-job");

    private JdbcSubmissionStore submissions;
    private JdbcTemplate jdbc;
    private TransactionTemplate host;
    private TransactionTemplate nested;
    private final List<String> durable = new ArrayList<>();

    @BeforeEach
    void setUp() {
        var source = new JdbcDataSource();
        source.setURL("jdbc:h2:mem:submission-savepoints-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        new ResourceDatabasePopulator(new ClassPathResource("schema-h2.sql")).execute(source);
        var dialect = new H2JdbcDelegate();
        new JdbcJobStore(source, Clock.fixed(NOW, ZoneOffset.UTC), dialect).upsert(
                JobDefinition.of(JOB.value(), new OnDemandSpec(), JobOptions.defaults(), Misfire.IGNORE, false));
        submissions = new JdbcSubmissionStore(source, dialect, Duration.ofDays(7));
        jdbc = new JdbcTemplate(source);
        var manager = new DataSourceTransactionManager(source);
        host = new TransactionTemplate(manager);
        nested = new TransactionTemplate(manager);
        nested.setPropagationBehavior(TransactionDefinition.PROPAGATION_NESTED);
    }

    @Test
    void enclosingNestedRollbackDiscardsCallbacksRegisteredAfterItsSavepoint() {
        host.executeWithoutResult(_ -> {
            accept("before");
            nested.executeWithoutResult(scope -> {
                accept("rolled-back");
                scope.setRollbackOnly();
            });
            accept("after");
            assertThat(durable).isEmpty();
        });

        assertThat(durable).containsExactly("before", "after");
        assertStored("before", "after");
    }

    @Test
    void enclosingRollbackDiscardsTheFirstCallbackAndItsWholeBatch() {
        host.executeWithoutResult(_ -> nested.executeWithoutResult(scope -> {
            String batch = TestIds.value("rolled-back-batch");
            submissions.accept(List.of(execution("batch-a", batch), execution("batch-b", batch)),
                    new SubmissionStore.NewBatch(batch, "nested batch"), () -> durable.add("batch"));
            scope.setRollbackOnly();
        }));

        assertThat(durable).isEmpty();
        assertStored();
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM mohs_batches", Long.class)).isZero();
    }

    @Test
    void programmaticSavepointFromBeforeTheFirstAcceptanceDiscardsItsCallback() {
        host.executeWithoutResult(scope -> {
            Object savepoint = scope.createSavepoint();
            accept("rolled-back");
            scope.rollbackToSavepoint(savepoint);
            scope.releaseSavepoint(savepoint);
            accept("survivor");
        });

        assertThat(durable).containsExactly("survivor");
        assertStored("survivor");
    }

    @Test
    void innerRollbackPreservesEarlierCallbacksButOuterRollbackCanStillDiscardThem() {
        host.executeWithoutResult(_ -> {
            accept("outside");
            nested.executeWithoutResult(outer -> {
                accept("outer");
                nested.executeWithoutResult(inner -> {
                    accept("inner");
                    inner.setRollbackOnly();
                });
                accept("outer-after-inner");
                outer.setRollbackOnly();
            });
        });

        assertThat(durable).containsExactly("outside");
        assertStored("outside");
    }

    @Test
    void aLaterEmptySavepointRollbackPreservesACommittedAcceptance() {
        host.executeWithoutResult(scope -> {
            accept("survivor");
            Object savepoint = scope.createSavepoint();
            scope.rollbackToSavepoint(savepoint);
            scope.releaseSavepoint(savepoint);
        });

        assertThat(durable).containsExactly("survivor");
        assertStored("survivor");
    }

    private void accept(String name) {
        submissions.accept(List.of(execution(name, null)), null, () -> durable.add(name));
    }

    private static SubmissionStore.NewExecution execution(String name, String batch) {
        return new SubmissionStore.NewExecution(ExecutionId.of(TestIds.value(name)), JOB, 0, 20,
                NOW, NOW, "test", batch, null, new JobPayload("{}", JOB.value()), SubmissionStore.Origin.MANUAL, null);
    }

    private void assertStored(String... names) {
        var expected = java.util.Arrays.stream(names).map(TestIds::value).toList();
        assertThat(jdbc.queryForList("SELECT execution_id FROM mohs_execution", String.class))
                .containsExactlyInAnyOrderElementsOf(expected);
        assertThat(jdbc.queryForList("SELECT execution_id FROM mohs_work", String.class))
                .containsExactlyInAnyOrderElementsOf(expected);
    }
}
