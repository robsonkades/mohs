package io.mohs.engine;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.sql.DataSource;

import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

import tools.jackson.databind.json.JsonMapper;

import io.mohs.core.definition.JobDefinition;
import io.mohs.core.execution.Attempt;
import io.mohs.core.execution.ExecutionId;
import io.mohs.core.execution.ExecutionState;
import io.mohs.core.job.JobKey;
import io.mohs.jdbc.JdbcExecutionStore;
import io.mohs.jdbc.JdbcJobStore;
import io.mohs.jdbc.JdbcTimestamps;
import io.mohs.jdbc.dialect.H2JdbcDialect;
import io.mohs.test.MutableClock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

/**
 * O group commit da conclusão (ADR-0047) sobre o store real (H2) — cada
 * gatilho de flush, o fallback e o dreno do close são comportamento
 * observável; sincronização por latch/timeout, nunca sleep.
 */
class CompletionBatcherTest {

    record Handler() {
    }

    private static final Instant NOW = Instant.parse("2026-08-14T12:00:00Z");
    private static final Duration LONG_INTERVAL = Duration.ofMinutes(5);

    private DataSource dataSource;
    private MutableClock clock;
    private JdbcJobStore jobStore;
    private JdbcExecutionStore executionStore;
    private CompletionBatcher batcher;

    @BeforeEach
    void setUp() {
        JdbcDataSource h2 = new JdbcDataSource();
        h2.setURL("jdbc:h2:mem:completion-batcher-test-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        h2.setUser("sa");
        h2.setPassword("");
        new ResourceDatabasePopulator(new ClassPathResource("schema-h2.sql")).execute(h2);
        dataSource = h2;
        clock = new MutableClock(NOW, ZoneId.of("UTC"));
        jobStore = new JdbcJobStore(dataSource, clock);
        executionStore = new JdbcExecutionStore(dataSource, clock, JsonMapper.builder().build(), new H2JdbcDialect());
        jobStore.upsert(JobDefinition.of("welcome-email", Handler.class, spec -> spec.onDemand()));
    }

    @AfterEach
    void tearDown() {
        if (batcher != null) {
            batcher.close();
        }
    }

    /** Cria e inicia o batcher no campo — {@code tearDown} fecha o que o teste não fechou. */
    private void startBatcher(ExecutionStore store, int flushSize, Duration flushInterval) {
        batcher = new CompletionBatcher(store, jobStore, flushSize, flushInterval);
        batcher.start();
    }

    private void seedRunningExecution(String id) {
        new JdbcTemplate(dataSource).update("""
                INSERT INTO mohs_executions (
                    id, job_key, state, scheduled_at, actor, node_id, lease_expires_at, payload, payload_type, created_at)
                VALUES (?, 'welcome-email', 'RUNNING', ?, 'test', 'node-a', ?, '{}', 'java.lang.Object', ?)
                """, id, JdbcTimestamps.toUtcTimestamp(NOW), JdbcTimestamps.toUtcTimestamp(NOW.plusSeconds(30)),
                JdbcTimestamps.toUtcTimestamp(NOW));
    }

    private ExecutionStore.CompletionRequest successRequest(String id) {
        Attempt attempt = new Attempt(1, NOW, NOW, ExecutionState.SUCCEEDED, null);
        return new ExecutionStore.CompletionRequest(ExecutionId.of(id), JobKey.of("welcome-email"), attempt,
                ExecutionState.SUCCEEDED);
    }

    private ExecutionState stateOf(String id) {
        return ExecutionState.valueOf(new JdbcTemplate(dataSource).queryForObject(
                "SELECT state FROM mohs_executions WHERE id = ?", String.class, id));
    }

    /** Gatilho de N: o intervalo é longo de propósito — se o flush dependesse dele, o await estouraria. */
    @Test
    void flushesWhenTheBatchFillsBeforeTheInterval() throws Exception {
        seedRunningExecution("exec-1");
        seedRunningExecution("exec-2");
        startBatcher(executionStore, 2, LONG_INTERVAL);
        CountDownLatch delivered = new CountDownLatch(2);

        batcher.submit(successRequest("exec-1"), completion -> delivered.countDown());
        batcher.submit(successRequest("exec-2"), completion -> delivered.countDown());

        assertThat(delivered.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.SUCCEEDED);
        assertThat(stateOf("exec-2")).isEqualTo(ExecutionState.SUCCEEDED);
    }

    /** Gatilho de T: lote longe de encher — o resultado fica durável no intervalo, não espera vizinhos. */
    @Test
    void flushesOnTheIntervalWhenTheBatchDoesNotFill() throws Exception {
        seedRunningExecution("exec-1");
        startBatcher(executionStore, 100, Duration.ofMillis(50));
        CountDownLatch delivered = new CountDownLatch(1);

        batcher.submit(successRequest("exec-1"), completion -> delivered.countDown());

        assertThat(delivered.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.SUCCEEDED);
    }

    /** O veredito do CAS atravessa o lote: quem já não estava RUNNING recebe NOT_APPLIED, nunca silêncio. */
    @Test
    void deliversNotAppliedWhenTheCasLoses() throws Exception {
        seedRunningExecution("exec-1");
        new JdbcTemplate(dataSource).update("UPDATE mohs_executions SET state = 'FAILED' WHERE id = 'exec-1'");
        startBatcher(executionStore, 1, LONG_INTERVAL);
        ConcurrentLinkedQueue<ExecutionStore.Completion> outcomes = new ConcurrentLinkedQueue<>();
        CountDownLatch delivered = new CountDownLatch(1);

        batcher.submit(successRequest("exec-1"), completion -> {
            outcomes.add(completion);
            delivered.countDown();
        });

        assertThat(delivered.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(outcomes).containsExactly(ExecutionStore.Completion.NOT_APPLIED);
    }

    /** Falha do flush em lote não descarta resultado nenhum: recai na conclusão individual, mesma transação de sempre. */
    @Test
    void fallsBackToPerResultCompletionWhenTheBatchFlushFails() throws Exception {
        seedRunningExecution("exec-1");
        seedRunningExecution("exec-2");
        ExecutionStore failingOnce = mock(ExecutionStore.class, delegatesTo(executionStore));
        doThrow(new IllegalStateException("simulated flush failure")).when(failingOnce).completeAll(anyList(), any());
        startBatcher(failingOnce, 2, LONG_INTERVAL);
        CountDownLatch delivered = new CountDownLatch(2);

        batcher.submit(successRequest("exec-1"), completion -> delivered.countDown());
        batcher.submit(successRequest("exec-2"), completion -> delivered.countDown());

        assertThat(delivered.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.SUCCEEDED);
        assertThat(stateOf("exec-2")).isEqualTo(ExecutionState.SUCCEEDED);
    }

    /** O close é o dreno do shutdown: o que estava na fila fica durável antes de ele retornar. */
    @Test
    void closeDrainsWhatIsStillQueued() {
        seedRunningExecution("exec-1");
        startBatcher(executionStore, 100, LONG_INTERVAL);
        AtomicBoolean deliveredBeforeCloseReturned = new AtomicBoolean();

        batcher.submit(successRequest("exec-1"), completion -> deliveredBeforeCloseReturned.set(true));
        batcher.close();

        assertThat(deliveredBeforeCloseReturned).isTrue();
        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.SUCCEEDED);
    }

    /** Zumbi que termina depois do shutdown não perde o resultado: submit pós-close conclui síncrono, na thread chamadora. */
    @Test
    void submitAfterCloseCompletesSynchronously() {
        seedRunningExecution("exec-1");
        startBatcher(executionStore, 100, LONG_INTERVAL);
        batcher.close();
        List<ExecutionStore.Completion> outcomes = new ArrayList<>();

        batcher.submit(successRequest("exec-1"), outcomes::add);

        assertThat(outcomes).hasSize(1);
        assertThat(outcomes.get(0).applied()).isTrue();
        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.SUCCEEDED);
    }
}
