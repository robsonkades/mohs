package io.mohs.engine;

import java.time.Instant;
import java.time.ZoneId;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

import tools.jackson.databind.json.JsonMapper;

import io.mohs.core.Mohs;
import io.mohs.core.MohsLifecycle;
import io.mohs.core.definition.JobDefinition;
import io.mohs.core.definition.JobSpec;
import io.mohs.core.event.Enqueued;
import io.mohs.core.execution.Execution;
import io.mohs.jdbc.JdbcExecutionStore;
import io.mohs.jdbc.JdbcJobStore;
import io.mohs.jdbc.JdbcNodeStore;
import io.mohs.jdbc.dialect.H2JdbcDialect;
import io.mohs.test.MutableClock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

/**
 * O terminal de {@code ScheduleCommand} contra as portas JDBC reais (H2) —
 * o contrato do Idempotent Receiver (EIP) depende do índice único
 * {@code uq_mohs_executions_idem} do schema, então mock de store não
 * provaria nada aqui.
 */
class ScheduleCommandImplTest {

    private static final Instant NOW = Instant.parse("2026-08-15T12:00:00Z");

    record Handler() {
    }

    private DataSource dataSource;
    private Mohs mohs;

    @BeforeEach
    void setUp() {
        dataSource = freshH2DataSource();
        MutableClock clock = new MutableClock(NOW, ZoneId.of("UTC"));
        JdbcJobStore jobStore = new JdbcJobStore(dataSource, clock);
        JdbcExecutionStore executionStore = new JdbcExecutionStore(dataSource, clock, JsonMapper.builder().build(), new H2JdbcDialect());
        mohs = new MohsImpl(jobStore, executionStore, new JdbcNodeStore(dataSource), mock(RateLimitStore.class), new HandlerRegistry(), clock, mock(MohsLifecycle.class), mock(BatchStore.class), new BatchCompletionCallbacks());
        mohs.define(JobDefinition.of("welcome-email", Handler.class, JobSpec::onDemand));
    }

    private static DataSource freshH2DataSource() {
        JdbcDataSource h2 = new JdbcDataSource();
        h2.setURL("jdbc:h2:mem:schedule-command-test-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        h2.setUser("sa");
        h2.setPassword("");
        new ResourceDatabasePopulator(new ClassPathResource("schema-h2.sql")).execute(h2);
        return h2;
    }

    private int executionCount() {
        Integer count = new JdbcTemplate(dataSource).queryForObject("SELECT COUNT(*) FROM mohs_executions", Integer.class);
        return count == null ? 0 : count;
    }

    @Test
    void schedulingWithAnUnknownJobFailsWithAnErrorThatTeaches() {
        assertThatThrownBy(() -> mohs.schedule("ghost", "payload").now())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Mohs.define");
    }

    /**
     * ADR-0035: o actor do scheduler é load-bearing (rearme fixed-delay, cura
     * do upsert) — agendamento manual jamais pode se passar pelo motor. A
     * rejeição é case/espaço-insensível porque a cura compara actor no BANCO,
     * cuja collation default (MySQL/SQL Server) é case-insensitive: "Scheduler"
     * passaria por um guard exato e ainda contaria como ocorrência do scheduler
     * no predicado SQL.
     */
    @Test
    void reservedSchedulerActorIsRejectedInAnyCaseWithAnErrorThatTeaches() {
        for (String forged : new String[] {Execution.SCHEDULER_ACTOR, "Scheduler", " scheduler ", "SCHEDULER"}) {
            assertThatThrownBy(() -> mohs.schedule("welcome-email", "hello").as(forged))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("reserved for engine-fired occurrences");
        }
    }

    /** O caso de uso do header: cliente reenvia o POST após timeout de rede — mesmo recibo, zero duplicação. */
    @Test
    void sameIdempotencyKeyReturnsTheOriginalReceiptWithoutDuplicating() {
        Enqueued first = mohs.schedule("welcome-email", "hello").idempotencyKey("req-1").now();

        Enqueued retry = mohs.schedule("welcome-email", "hello").idempotencyKey("req-1").now();

        assertThat(retry.executionId()).isEqualTo(first.executionId());
        assertThat(retry.scheduledAt()).isEqualTo(first.scheduledAt());
        assertThat(executionCount()).isEqualTo(1);
    }

    @Test
    void distinctIdempotencyKeysProduceDistinctExecutions() {
        Enqueued first = mohs.schedule("welcome-email", "hello").idempotencyKey("req-1").now();
        Enqueued second = mohs.schedule("welcome-email", "hello").idempotencyKey("req-2").now();

        assertThat(second.executionId()).isNotEqualTo(first.executionId());
        assertThat(executionCount()).isEqualTo(2);
    }

    @Test
    void executionsWithoutAnIdempotencyKeyNeverCollide() {
        mohs.schedule("welcome-email", "hello").now();
        mohs.schedule("welcome-email", "hello").now();

        assertThat(executionCount()).isEqualTo(2);
    }

    /** A corrida real do retry: dois POSTs simultâneos com a mesma chave — o banco decide, os dois recebem o mesmo recibo. */
    @Test
    void concurrentSchedulesWithTheSameKeyConvergeOnASingleExecution() throws Exception {
        CyclicBarrier barrier = new CyclicBarrier(2);
        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        Callable<Enqueued> schedule = () -> {
            barrier.await();
            return mohs.schedule("welcome-email", "hello").idempotencyKey("req-1").now();
        };

        Future<Enqueued> futureA = executor.submit(schedule);
        Future<Enqueued> futureB = executor.submit(schedule);
        Enqueued a = futureA.get(10, TimeUnit.SECONDS);
        Enqueued b = futureB.get(10, TimeUnit.SECONDS);
        executor.shutdown();
        assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();

        assertThat(a.executionId()).isEqualTo(b.executionId());
        assertThat(executionCount()).isEqualTo(1);
    }
}
