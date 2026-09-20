package io.mohs.store.jdbc;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.jdbc.datasource.AbstractDataSource;
import io.mohs.core.execution.ExecutionState;
import io.mohs.engine.LeaseStore;
import io.mohs.test.TestIds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JdbcReadModelAtomicityTest {
    private static final Instant NOW = Instant.parse("2026-09-09T12:00:00Z");

    @ParameterizedTest(name = "{0}")
    @MethodSource("io.mohs.store.jdbc.JdbcJobBindingTest#backends")
    void aPendingRowWithoutWorkIsAnInvariantFailure(JdbcJobBindingTest.Backend backend) {
        var fixture = new JdbcOwnershipProtocolTest.Fixture(backend);
        var id = fixture.enqueue(TestIds.value("orphan-pending"));
        assertThat(fixture.history.find(id, NOW).orElseThrow().state()).isEqualTo(ExecutionState.ENQUEUED);
        fixture.jdbc.update("DELETE FROM mohs_work WHERE work_state = 'READY' AND  execution_id = ?", id.value());
        assertThatThrownBy(() -> fixture.history.find(id, NOW))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("exactly one active work record");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("io.mohs.store.jdbc.JdbcJobBindingTest#backends")
    void aCompletionBeforeCommitStillReadsAsOwnedAndAfterCommitAsTerminal(JdbcJobBindingTest.Backend backend) throws Exception {
        var fixture = new JdbcOwnershipProtocolTest.Fixture(backend);
        String node = TestIds.value("read-model-session");
        fixture.register(node);
        var id = fixture.enqueue(TestIds.value("read-model-commit"));
        var grant = fixture.queue.claim(0, node, 1, List.of(), NOW).getFirst();
        var atCommit = new CountDownLatch(1);
        var allowCommit = new CountDownLatch(1);
        var source = new BeforeCommit(backend.dataSource(), () -> {
            atCommit.countDown();
            await(allowCommit);
        });
        var completion = new LeaseStore.CompletionResult(id, fixture.job, node, grant.claimToken(), 1,
                NOW, NOW, ExecutionState.SUCCEEDED, null, null, null);
        try (var threads = Executors.newVirtualThreadPerTaskExecutor()) {
            var writer = threads.submit(() -> new JdbcLeaseStore(source, backend.delegate()).complete(List.of(completion)));
            try {
                assertThat(atCommit.await(5, TimeUnit.SECONDS)).isTrue();
                var reader = threads.submit(() -> fixture.history.find(id, NOW).orElseThrow());
                assertThat(reader.get(2, TimeUnit.SECONDS).state()).isEqualTo(ExecutionState.RUNNING);
            } finally {
                allowCommit.countDown();
            }
            assertThat(writer.get(5, TimeUnit.SECONDS).get(id).owned()).isTrue();
        }
        assertThat(fixture.history.find(id, NOW).orElseThrow().state()).isEqualTo(ExecutionState.SUCCEEDED);
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) throw new IllegalStateException("test commit barrier timed out");
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(failure);
        }
    }

    static final class BeforeCommit extends AbstractDataSource {
        private final DataSource delegate;
        private final Runnable barrier;
        BeforeCommit(DataSource delegate, Runnable barrier) { this.delegate = delegate; this.barrier = barrier; }
        @Override public Connection getConnection() throws SQLException { return wrap(delegate.getConnection()); }
        @Override public Connection getConnection(String user, String password) throws SQLException { return wrap(delegate.getConnection(user, password)); }
        private Connection wrap(Connection connection) {
            return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[]{Connection.class}, (_, method, args) -> {
                if (method.getName().equals("commit")) barrier.run();
                try { return method.invoke(connection, args); }
                catch (InvocationTargetException failure) { throw failure.getCause(); }
            });
        }
    }
}
