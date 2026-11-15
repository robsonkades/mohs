package io.mohs.store.jdbc;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import javax.sql.DataSource;

import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.datasource.AbstractDataSource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

import io.mohs.core.EngineState;
import io.mohs.core.execution.ExecutionId;
import io.mohs.core.execution.ExecutionState;
import io.mohs.core.job.JobKey;
import io.mohs.engine.LeaseStore;
import io.mohs.engine.SubmissionStore;
import io.mohs.store.jdbc.delegate.H2JdbcDelegate;
import io.mohs.test.InputFixtures;
import io.mohs.test.TestIds;

import static org.assertj.core.api.Assertions.assertThat;

class JdbcCompletionQueryCostTest {
    private static final Instant NOW = Instant.parse("2026-09-09T12:00:00Z");

    @ParameterizedTest
    @ValueSource(ints = {16, 64})
    void oneManualBatchNeedsBoundedQueriesAndStillElectsItsLastTerminalMember(int total) {
        var source = new JdbcDataSource();
        source.setURL("jdbc:h2:mem:completion-query-cost-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        new ResourceDatabasePopulator(new ClassPathResource("schema-h2.sql")).execute(source);
        var dialect = new H2JdbcDelegate();
        var key = JobKey.of("completion-cost");
        String batchId = TestIds.value("completion-cost-batch");
        String nodeId = TestIds.value("completion-cost-node");
        var requests = IntStream.range(0, total).mapToObj(i -> new SubmissionStore.NewExecution(
                ExecutionId.of(TestIds.value("completion-cost-" + i)), key, 0, 20, NOW, NOW, "test", batchId, null,
                InputFixtures.encode(key, "input"), SubmissionStore.Origin.MANUAL, null)).toList();
        TestSubmissions.declare(source, dialect, requests);
        new JdbcSubmissionStore(source, dialect, Duration.ofDays(7)).accept(requests,
                new SubmissionStore.NewBatch(batchId, "cost"), () -> {});
        TestSessions.seed(source, dialect, nodeId, EngineState.RUNNING, Duration.ofMinutes(5));
        var claims = new JdbcWorkQueue(source, dialect).claim(0, nodeId, total, List.of(), NOW);
        assertThat(claims).hasSize(total);
        var results = IntStream.range(0, total).mapToObj(i -> {
            var claim = claims.get(i);
            var outcome = switch (i % 3) {
                case 0 -> ExecutionState.SUCCEEDED;
                case 1 -> ExecutionState.FAILED;
                default -> ExecutionState.CANCELLED;
            };
            return new LeaseStore.CompletionResult(claim.executionId(), key, nodeId, claim.claimToken(), 1,
                    NOW, NOW, outcome, null, null, null);
        }).toList();

        var counted = new CountingDataSource(source);
        var leases = new JdbcLeaseStore(counted, dialect);
        var completed = leases.complete(results.reversed());
        assertThat(completed.values()).allMatch(LeaseStore.Completion::owned);
        assertThat(counted.statements.get()).as("queries for %s manual batch members", total).isLessThan(16);
        var lastId = results.stream().map(LeaseStore.CompletionResult::executionId)
                .max(Comparator.comparing(ExecutionId::value)).orElseThrow();
        assertThat(completed.entrySet().stream().filter(entry -> entry.getValue().closedBatch() != null)
                .map(java.util.Map.Entry::getKey)).containsExactly(lastId);
        var counters = new JdbcBatchStore(source, dialect).find(batchId).orElseThrow();
        assertThat(counters.succeeded()).isEqualTo((total + 2) / 3);
        assertThat(counters.failed()).isEqualTo(total - counters.succeeded());
        assertThat(counters.pending()).isZero();
        assertThat(leases.complete(results).values()).allMatch(LeaseStore.Completion::alreadyApplied);
        assertThat(new JdbcBatchStore(source, dialect).find(batchId)).contains(counters);
    }

    private static final class CountingDataSource extends AbstractDataSource {
        private final DataSource delegate;
        final AtomicInteger statements = new AtomicInteger();

        CountingDataSource(DataSource delegate) {
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
            return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[]{Connection.class},
                    (_, method, arguments) -> {
                if (method.getName().equals("prepareStatement")) statements.incrementAndGet();
                try {
                    return method.invoke(connection, arguments);
                } catch (InvocationTargetException failure) {
                    throw failure.getCause();
                }
            });
        }
    }
}
