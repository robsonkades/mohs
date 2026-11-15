package io.mohs.store.jdbc;

import io.mohs.engine.SubmissionStore;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.sql.DataSource;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.jdbc.datasource.AbstractDataSource;
import io.mohs.core.execution.ExecutionState;
import io.mohs.engine.LeaseStore;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** A commit can succeed while its response is lost; retrying the same grant must not repeat accounting. */
class JdbcCommitUncertaintyTest {
    private static final Instant NOW = Instant.parse("2026-09-09T12:00:00Z");

    @ParameterizedTest(name = "{0}")
    @MethodSource("io.mohs.store.jdbc.JdbcJobBindingTest#backends")
    void lostCatalogAndJoinResponsesReconcileTheSamePublicationAndSession(JdbcJobBindingTest.Backend backend) {
        var failing = new LostCommitResponse(backend.dataSource());
        var key = io.mohs.core.job.JobKey.of("uncertain-catalog");
        var catalog = new io.mohs.engine.ExecutableCatalog(java.util.Map.of(key, key.value()));
        var definition = io.mohs.core.definition.JobDefinition.of(key.value(), new io.mohs.core.schedule.OnDemandSpec(),
                io.mohs.core.definition.JobOptions.defaults(), io.mohs.core.schedule.Misfire.IGNORE, false);
        var catalogs = new JdbcCatalogStore(failing, Clock.systemUTC(), backend.delegate());
        failing.loseNextCommitResponse.set(true);
        assertThatThrownBy(() -> catalogs.reconcile(catalog, List.of(definition), io.mohs.engine.CatalogStore.Conflict.OVERRIDE))
                .hasRootCauseMessage("simulated lost commit response");
        catalogs.reconcile(catalog, List.of(definition), io.mohs.engine.CatalogStore.Conflict.OVERRIDE);
        assertThat(new org.springframework.jdbc.core.JdbcTemplate(backend.dataSource())
                .queryForObject("SELECT fingerprint FROM mohs_catalog", String.class)).isEqualTo(catalog.fingerprint());

        var nodes = new JdbcNodeStore(failing, backend.delegate());
        String id = io.mohs.test.TestIds.value("uncertain-join");
        failing.loseNextCommitResponse.set(true);
        assertThatThrownBy(() -> nodes.register(id, io.mohs.core.EngineState.RUNNING, java.time.Duration.ofMinutes(5), catalog))
                .hasRootCauseMessage("simulated lost commit response");
        var committed = nodes.findAll().getFirst();
        assertThat(nodes.register(id, io.mohs.core.EngineState.RUNNING, java.time.Duration.ofMinutes(5), catalog))
                .isEqualTo(committed);
        assertThat(nodes.findAll()).containsExactly(committed);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("io.mohs.store.jdbc.JdbcJobBindingTest#backends")
    void lostSubmissionResponseLeavesACompleteAcceptanceReconciledByItsReceipt(JdbcJobBindingTest.Backend backend) {
        var failing = new LostCommitResponse(backend.dataSource());
        var f = new JdbcOwnershipProtocolTest.Fixture(new JdbcJobBindingTest.Backend(backend.name(), failing, backend.delegate()));
        var batches = new JdbcBatchStore(failing, backend.delegate());
        var submissions = new JdbcSubmissionStore(failing, backend.delegate(), java.time.Duration.ofDays(7));
        var id = io.mohs.core.execution.ExecutionId.of("01991337-0000-7000-8000-00000000008c");
        var input = new io.mohs.engine.SubmissionStore.NewExecution(
                id, f.job, 0, 20, NOW, NOW, "test", null, "request", io.mohs.test.InputFixtures.encode(f.job, "input"),
                io.mohs.engine.SubmissionStore.Origin.MANUAL, null);
        var delivered = new java.util.concurrent.atomic.AtomicInteger();
        failing.loseNextCommitResponse.set(true);

        assertThatThrownBy(() -> submissions.accept(List.of(input), null, delivered::incrementAndGet))
                .hasRootCauseMessage("simulated lost commit response");
        assertThat(f.history.findReceiptByIdempotencyKey(f.job, "request").orElseThrow().executionId()).isEqualTo(id);
        for (String table : List.of("mohs_execution", "mohs_work", "mohs_idempotency")) {
            assertThat(f.jdbc.queryForObject("SELECT COUNT(*) FROM " + table, Long.class)).as(table).isEqualTo(1L);
        }
        assertThat(delivered).hasValue(0); // No receipt response/event is invented after the uncertain commit.
        assertThatThrownBy(() -> submissions.accept(List.of(input), null, delivered::incrementAndGet))
                .isInstanceOf(org.springframework.dao.DuplicateKeyException.class);
        assertThat(f.history.findReceiptByIdempotencyKey(f.job, "request").orElseThrow().executionId()).isEqualTo(id);
        assertThat(f.jdbc.queryForObject("SELECT COUNT(*) FROM mohs_work WHERE work_state = 'READY'", Long.class)).isEqualTo(1L);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("io.mohs.store.jdbc.JdbcJobBindingTest#backends")
    void lostClaimResponseLeavesARecoverableGrantWithoutDispatchingUnconfirmedWork(JdbcJobBindingTest.Backend backend) {
        var failing = new LostCommitResponse(backend.dataSource());
        var f = new JdbcOwnershipProtocolTest.Fixture(new JdbcJobBindingTest.Backend(backend.name(), failing, backend.delegate()));
        f.register("01991337-0000-7000-8000-000000000006");
        var id = f.enqueue("01991337-0000-7000-8000-00000000008a");
        failing.loseNextCommitResponse.set(true);
        assertThatThrownBy(() -> f.queue.claim(0, "01991337-0000-7000-8000-000000000006", 1, List.of(), NOW))
                .hasRootCauseMessage("simulated lost commit response");
        var grant = f.leases.findByNodes(List.of("01991337-0000-7000-8000-000000000006")).getFirst();
        assertThat(grant.executionId()).isEqualTo(id);
        f.expire("01991337-0000-7000-8000-000000000006");
        f.nodes.revokeExpired("01991337-0000-7000-8000-000000000006");
        var retry = new LeaseStore.CompletionResult(id, f.job, "01991337-0000-7000-8000-000000000006", grant.claimToken(), 1, NOW, NOW,
                ExecutionState.FAILED, "LostClaimResponse", "no confirmed dispatch", NOW);
        assertThat(f.leases.reclaim(List.of(retry)).get(id).owned()).isTrue();
        f.register("01991337-0002-7000-8000-00000000009b");
        var recovered = f.queue.claim(0, "01991337-0002-7000-8000-00000000009b", 1, List.of(), NOW).getFirst();
        assertThat(recovered.executionId()).isEqualTo(id);
        assertThat(recovered.claimToken()).isNotEqualTo(grant.claimToken());
        assertThat(recovered.attemptNumber()).isEqualTo(2);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("io.mohs.store.jdbc.JdbcJobBindingTest#backends")
    void lostCompletionResponseIsRecognizedWithoutCountingTheBatchAgain(JdbcJobBindingTest.Backend backend) {
        var failing = new LostCommitResponse(backend.dataSource());
        var f = new JdbcOwnershipProtocolTest.Fixture(new JdbcJobBindingTest.Backend(backend.name(), failing, backend.delegate()));
        var batches = new JdbcBatchStore(failing, backend.delegate());
        TestWork.batch(failing, backend.delegate(), "01991337-0000-7000-8000-000000000089", "commit response", 1);
        f.register("01991337-0000-7000-8000-000000000006");
        var id = f.enqueue("01991337-0000-7000-8000-00000000008b");
        f.jdbc.update("UPDATE mohs_execution SET batch_id = ? WHERE execution_id = ?", "01991337-0000-7000-8000-000000000089", id.value());
        var grant = f.queue.claim(0, "01991337-0000-7000-8000-000000000006", 1, List.of(), NOW).getFirst();
        var result = new LeaseStore.CompletionResult(id, f.job, "01991337-0000-7000-8000-000000000006", grant.claimToken(), 1,
                NOW, NOW, ExecutionState.SUCCEEDED, null, null, null);
        failing.loseNextCommitResponse.set(true);

        assertThatThrownBy(() -> f.leases.complete(List.of(result))).hasRootCauseMessage("simulated lost commit response");
        assertThat(f.history.find(id, NOW).orElseThrow().state()).isEqualTo(ExecutionState.SUCCEEDED);
        assertThat(f.leases.complete(List.of(result)).get(id)).isEqualTo(LeaseStore.Completion.ALREADY_APPLIED);
        assertThat(f.jdbc.queryForObject("SELECT succeeded FROM mohs_batches WHERE id = ?", Long.class, "01991337-0000-7000-8000-000000000089")).isEqualTo(1L);
        assertThat(f.leases.findByNodes(List.of("01991337-0000-7000-8000-000000000006"))).isEmpty();
    }

    private static final class LostCommitResponse extends AbstractDataSource {
        private final DataSource delegate;
        private final AtomicBoolean loseNextCommitResponse = new AtomicBoolean();

        LostCommitResponse(DataSource delegate) {
            this.delegate = delegate;
        }

        @Override
        public Connection getConnection() throws SQLException {
            return wrap(delegate.getConnection());
        }

        @Override
        public Connection getConnection(String username, String password) throws SQLException {
            return wrap(delegate.getConnection(username, password));
        }

        private Connection wrap(Connection connection) {
            return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[] {Connection.class},
                    (_, method, args) -> {
                Object result;
                try {
                    result = method.invoke(connection, args);
                } catch (InvocationTargetException failure) {
                    throw failure.getCause();
                }
                if (method.getName().equals("commit") && loseNextCommitResponse.compareAndSet(true, false)) {
                    throw new SQLException("simulated lost commit response", "08006");
                }
                return result;
            });
        }
    }
}
