package io.mohs.store.jdbc;

import java.time.Instant;
import java.time.ZoneId;
import java.util.List;

import javax.sql.DataSource;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import tools.jackson.databind.json.JsonMapper;

import io.mohs.core.Mohs;
import io.mohs.core.MohsLifecycle;
import io.mohs.core.definition.JobDefinition;
import io.mohs.core.definition.JobSpec;
import io.mohs.core.event.Enqueued;
import io.mohs.core.resource.MohsRunner;
import io.mohs.engine.BatchCompletionCallbacks;
import io.mohs.engine.HandlerRegistry;
import io.mohs.engine.MohsImpl;
import io.mohs.engine.RateLimitStore;
import io.mohs.engine.RunnerRegistry;
import io.mohs.store.jdbc.dialect.PostgresJdbcDialect;
import io.mohs.test.MutableClock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * O gêmeo Postgres do teste de savepoint de {@code ScheduleCommandImplTest}
 * (review S5.3): é AQUI que o bug do {@code REQUIRED} era pior — sem
 * savepoint, a violação de PK do dedup aborta a transação inteira
 * ({@code 25P02 current transaction is aborted}) e o caminho de
 * recuperação que lê o vencedor roda numa conexão morta. H2 prova a
 * metade cross-dialeto (rollback-only evitado); só o Postgres real prova
 * que a conexão continua sã DEPOIS do conflito.
 */
class ScheduleCommandPostgresTest {

    private static final Instant NOW = Instant.parse("2026-08-15T12:00:00Z");

    record Handler() {
    }

    private DataSource dataSource;
    private Mohs mohs;

    @BeforeEach
    void setUp() {
        dataSource = PostgresTestSupport.freshSchema();
        MutableClock clock = new MutableClock(NOW, ZoneId.of("UTC"));
        new PostgresPartitionManager(dataSource).ensureWeeklyPartitions(NOW);
        JdbcJobStore jobStore = new JdbcJobStore(dataSource, clock);
        JdbcBatchStore batchStore = new JdbcBatchStore(dataSource, clock);
        JdbcHistoryStore historyStore = new JdbcHistoryStore(dataSource, JsonMapper.builder().build(), new PostgresJdbcDialect());
        JdbcWorkQueue workQueue = new JdbcWorkQueue(dataSource, new PostgresJdbcDialect(), batchStore, clock);
        JdbcLeaseStore leaseStore = new JdbcLeaseStore(dataSource, new PostgresJdbcDialect(), batchStore);
        mohs = new MohsImpl(jobStore, workQueue, historyStore, leaseStore, new JdbcStoreTransactions(dataSource),
                new JdbcNodeStore(dataSource), mock(RateLimitStore.class), new HandlerRegistry(), clock,
                mock(MohsLifecycle.class), batchStore, new BatchCompletionCallbacks(),
                new RunnerRegistry(List.of(MohsRunner.io("io").build())), () -> { });
        mohs.define(JobDefinition.of("welcome-email", Handler.class, JobSpec::onDemand));
    }

    /** A prova que só o dialeto do 25P02 dá: conflito de PK → savepoint desfeito → a MESMA conexão lê o vencedor e o host commita. */
    @Test
    void duplicateIdempotencyKeyInsideAHostTransactionLeavesTheConnectionUsable() {
        TransactionTemplate host = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
        Enqueued[] receipts = new Enqueued[2];

        host.executeWithoutResult(_ -> {
            receipts[0] = mohs.schedule("welcome-email", "hello").idempotencyKey("req-1").now();
            receipts[1] = mohs.schedule("welcome-email", "hello").idempotencyKey("req-1").now();
        });

        assertThat(receipts[1].executionId()).isEqualTo(receipts[0].executionId());
        assertThat(new JdbcTemplate(dataSource).queryForObject(
                "SELECT COUNT(*) FROM mohs_execution", Integer.class)).isEqualTo(1);
    }
}
