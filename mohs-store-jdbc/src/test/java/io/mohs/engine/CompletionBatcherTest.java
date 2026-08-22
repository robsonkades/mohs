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

import io.mohs.core.definition.JobDefinition;
import io.mohs.core.execution.ExecutionId;
import io.mohs.core.execution.ExecutionState;
import io.mohs.core.job.JobKey;
import io.mohs.store.jdbc.JdbcBatchStore;
import io.mohs.store.jdbc.JdbcJobStore;
import io.mohs.store.jdbc.JdbcLeaseStore;
import io.mohs.store.jdbc.JdbcTimestamps;
import io.mohs.store.jdbc.JdbcWorkQueue;
import io.mohs.store.jdbc.dialect.H2JdbcDialect;
import io.mohs.test.MutableClock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

/**
 * O group commit da conclusão (ADR-0047, agora sobre {@link LeaseStore} —
 * Phase 5) contra o store real (H2) — cada gatilho de flush, o fallback e
 * o dreno do close são comportamento observável; sincronização por
 * latch/timeout, nunca sleep. A posse nasce pelo caminho real
 * (fila → claim), então o fence {@code (node_id, epoch)} dos resultados é
 * o de verdade.
 */
class CompletionBatcherTest {

    record Handler() {
    }

    private static final Instant NOW = Instant.parse("2026-08-14T12:00:00Z");
    private static final Duration LONG_INTERVAL = Duration.ofMinutes(5);
    private static final String NODE = "node-a";
    private static final long EPOCH = 1;

    private DataSource dataSource;
    private MutableClock clock;
    private JdbcJobStore jobStore;
    private JdbcLeaseStore leaseStore;
    private JdbcWorkQueue workQueue;
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
        JdbcBatchStore batchStore = new JdbcBatchStore(dataSource, clock);
        leaseStore = new JdbcLeaseStore(dataSource, new H2JdbcDialect(), batchStore);
        workQueue = new JdbcWorkQueue(dataSource, new H2JdbcDialect(), batchStore);
        jobStore.upsert(JobDefinition.of("welcome-email", Handler.class, spec -> spec.onDemand()));
    }

    @AfterEach
    void tearDown() {
        if (batcher != null) {
            batcher.close();
        }
    }

    /** Cria e inicia o batcher no campo — {@code tearDown} fecha o que o teste não fechou. */
    private void startBatcher(LeaseStore store, int flushSize, Duration flushInterval) {
        batcher = new CompletionBatcher(store, jobStore, flushSize, flushInterval);
        batcher.start();
    }

    /** História + fila + claim — a posse nasce pelo caminho real, com o fence (NODE, EPOCH). */
    private void seedLeasedExecution(String id) {
        new JdbcTemplate(dataSource).update("""
                INSERT INTO mohs_execution (execution_id, job_key, state, scheduled_at, created_at, actor, payload, payload_type)
                VALUES (?, 'welcome-email', 'PENDING', ?, ?, 'test', '{}', 'java.lang.Object')
                """, id, JdbcTimestamps.toUtcLocalDateTime(NOW), JdbcTimestamps.toUtcLocalDateTime(NOW));
        workQueue.offer(List.of(new WorkQueue.ReadyEntry(ExecutionId.of(id), JobKey.of("welcome-email"), 0, 20, 1, NOW.minusSeconds(1))));
        workQueue.claim(0, NODE, EPOCH, 10, List.of(), NOW);
    }

    private LeaseStore.CompletionResult successResult(String id) {
        return new LeaseStore.CompletionResult(ExecutionId.of(id), JobKey.of("welcome-email"), NODE, EPOCH, 1,
                NOW, NOW, ExecutionState.SUCCEEDED, null, null, ExecutionState.SUCCEEDED, NOW, null);
    }

    private ExecutionState stateOf(String id) {
        return ExecutionState.valueOf(new JdbcTemplate(dataSource).queryForObject(
                "SELECT state FROM mohs_execution WHERE execution_id = ?", String.class, id));
    }

    /** Gatilho de N: o intervalo é longo de propósito — se o flush dependesse dele, o await estouraria. */
    @Test
    void flushesWhenTheBatchFillsBeforeTheInterval() throws Exception {
        seedLeasedExecution("exec-1");
        seedLeasedExecution("exec-2");
        startBatcher(leaseStore, 2, LONG_INTERVAL);
        CountDownLatch delivered = new CountDownLatch(2);

        batcher.submit(successResult("exec-1"), completion -> delivered.countDown());
        batcher.submit(successResult("exec-2"), completion -> delivered.countDown());

        assertThat(delivered.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.SUCCEEDED);
        assertThat(stateOf("exec-2")).isEqualTo(ExecutionState.SUCCEEDED);
    }

    /** Gatilho de T: lote longe de encher — o resultado fica durável no intervalo, não espera vizinhos. */
    @Test
    void flushesOnTheIntervalWhenTheBatchDoesNotFill() throws Exception {
        seedLeasedExecution("exec-1");
        startBatcher(leaseStore, 100, Duration.ofMillis(50));
        CountDownLatch delivered = new CountDownLatch(1);

        batcher.submit(successResult("exec-1"), completion -> delivered.countDown());

        assertThat(delivered.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.SUCCEEDED);
    }

    /** O veredito do fence atravessa o lote: quem já perdeu a posse (reaper/requeue passou antes) recebe FENCED_OUT, nunca silêncio. */
    @Test
    void deliversFencedOutWhenTheIncarnationWasLost() throws Exception {
        seedLeasedExecution("exec-1");
        // a posse trocou de mãos: um reaper derrubou a lease e outro nó re-reivindicou
        new JdbcTemplate(dataSource).update("UPDATE mohs_lease SET node_id = 'node-b', epoch = 9 WHERE execution_id = 'exec-1'");
        startBatcher(leaseStore, 1, LONG_INTERVAL);
        ConcurrentLinkedQueue<LeaseStore.Completion> outcomes = new ConcurrentLinkedQueue<>();
        CountDownLatch delivered = new CountDownLatch(1);

        batcher.submit(successResult("exec-1"), completion -> {
            outcomes.add(completion);
            delivered.countDown();
        });

        assertThat(delivered.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(outcomes).containsExactly(LeaseStore.Completion.FENCED_OUT);
        // nada foi gravado pelo perdedor: advisory intacto, a lease da encarnação nova de pé
        assertThat(new JdbcTemplate(dataSource).queryForObject(
                "SELECT state FROM mohs_execution WHERE execution_id = 'exec-1'", String.class)).isEqualTo("PENDING");
        assertThat(new JdbcTemplate(dataSource).queryForObject(
                "SELECT node_id FROM mohs_lease WHERE execution_id = 'exec-1'", String.class)).isEqualTo("node-b");
    }

    /** Falha do flush em lote não descarta resultado nenhum: recai na conclusão individual, mesma transação de sempre. */
    @Test
    void fallsBackToPerResultCompletionWhenTheBatchFlushFails() throws Exception {
        seedLeasedExecution("exec-1");
        seedLeasedExecution("exec-2");
        LeaseStore failingOnce = mock(LeaseStore.class, delegatesTo(leaseStore));
        doThrow(new IllegalStateException("simulated flush failure"))
                .when(failingOnce).complete(argThatIsBatchOfTwo(), any());
        startBatcher(failingOnce, 2, LONG_INTERVAL);
        CountDownLatch delivered = new CountDownLatch(2);

        batcher.submit(successResult("exec-1"), completion -> delivered.countDown());
        batcher.submit(successResult("exec-2"), completion -> delivered.countDown());

        assertThat(delivered.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.SUCCEEDED);
        assertThat(stateOf("exec-2")).isEqualTo(ExecutionState.SUCCEEDED);
    }

    private static List<LeaseStore.CompletionResult> argThatIsBatchOfTwo() {
        return org.mockito.ArgumentMatchers.argThat(list -> list != null && list.size() == 2);
    }

    /** O guard por estado do reconcile (S5.5): id fica em trânsito entre o submit e o veredito, e some em TODO desfecho. */
    @Test
    void completionInTransitTracksSubmitToOutcome() throws Exception {
        seedLeasedExecution("exec-1");
        startBatcher(leaseStore, 100, LONG_INTERVAL); // lote longe de encher: só o close flusha
        CountDownLatch delivered = new CountDownLatch(1);

        batcher.submit(successResult("exec-1"), completion -> delivered.countDown());
        assertThat(batcher.completionInTransit(ExecutionId.of("exec-1"))).isTrue();

        batcher.close();
        assertThat(delivered.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(batcher.completionInTransit(ExecutionId.of("exec-1"))).isFalse();
    }

    /** O close é o dreno do shutdown: o que estava na fila fica durável antes de ele retornar. */
    @Test
    void closeDrainsWhatIsStillQueued() {
        seedLeasedExecution("exec-1");
        startBatcher(leaseStore, 100, LONG_INTERVAL);
        AtomicBoolean deliveredBeforeCloseReturned = new AtomicBoolean();

        batcher.submit(successResult("exec-1"), completion -> deliveredBeforeCloseReturned.set(true));
        batcher.close();

        assertThat(deliveredBeforeCloseReturned).isTrue();
        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.SUCCEEDED);
    }

    /** Zumbi que termina depois do shutdown não perde o resultado: submit pós-close conclui síncrono, na thread chamadora. */
    @Test
    void submitAfterCloseCompletesSynchronously() {
        seedLeasedExecution("exec-1");
        startBatcher(leaseStore, 100, LONG_INTERVAL);
        batcher.close();
        List<LeaseStore.Completion> outcomes = new ArrayList<>();

        batcher.submit(successResult("exec-1"), outcomes::add);

        assertThat(outcomes).hasSize(1);
        assertThat(outcomes.get(0).owned()).isTrue();
        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.SUCCEEDED);
    }
}
