package io.mohs.engine;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import io.mohs.core.Batch;
import io.mohs.core.EngineState;
import io.mohs.core.ExecutionQuery;
import io.mohs.core.JobSnapshot;
import io.mohs.core.Mohs;
import io.mohs.core.MohsLifecycle;
import io.mohs.core.NodeSnapshot;
import io.mohs.core.OverviewSnapshot;
import io.mohs.core.definition.DefinitionSource;
import io.mohs.core.definition.JobDefinition;
import io.mohs.core.definition.JobSpec;
import io.mohs.core.execution.Execution;
import io.mohs.core.execution.ExecutionId;
import io.mohs.core.execution.ExecutionState;
import io.mohs.core.execution.Priority;
import io.mohs.core.job.JobKey;
import io.mohs.core.job.JobRef;
import io.mohs.core.resource.MohsRunner;
import io.mohs.core.schedule.IntervalSpec;
import io.mohs.core.schedule.Misfire;
import io.mohs.core.schedule.OnDemandSpec;
import io.mohs.test.InMemoryJobStore;
import io.mohs.test.MutableClock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * Métodos de leitura de {@link MohsImpl} — a costura entre {@link Mohs}
 * e as portas {@link JobStore}/{@link WorkQueue}/{@link HistoryStore}/
 * {@link LeaseStore} que {@code io.mohs.rest} consome. {@link
 * InMemoryJobStore} real (comportamento de verdade, sem mock); as portas
 * da Phase 5 mockadas — a lógica de fila/derivação delas já tem suíte
 * própria nos {@code Jdbc*Test} de {@code io.mohs.store.jdbc}.
 */
class MohsImplTest {

    private static final Instant NOW = Instant.parse("2026-08-15T12:00:00Z");

    record Handler() {
    }

    private InMemoryJobStore jobStore;
    private WorkQueue workQueue;
    private HistoryStore historyStore;
    private LeaseStore leaseStore;
    private NodeStore nodeStore;
    private RateLimitStore rateLimitStore;
    private HandlerRegistry handlerRegistry;
    private BatchStore batchStore;
    private MohsImpl mohs;

    @BeforeEach
    void setUp() {
        MutableClock clock = new MutableClock(NOW, ZoneId.of("UTC"));
        jobStore = new InMemoryJobStore(clock);
        workQueue = mock(WorkQueue.class);
        historyStore = mock(HistoryStore.class);
        leaseStore = mock(LeaseStore.class);
        nodeStore = mock(NodeStore.class);
        rateLimitStore = mock(RateLimitStore.class);
        handlerRegistry = new HandlerRegistry();
        batchStore = mock(BatchStore.class);
        mohs = new MohsImpl(jobStore, workQueue, historyStore, leaseStore, work -> work.run(), nodeStore, rateLimitStore, handlerRegistry, clock, mock(MohsLifecycle.class), batchStore, new BatchCompletionCallbacks(), new RunnerRegistry(List.of(MohsRunner.io("io").build())), () -> { });
    }

    private static JobDefinition onDemand(String key) {
        return JobDefinition.of(key, Handler.class, JobSpec::onDemand);
    }

    private static JobDefinition everyMinute(String key) {
        return JobDefinition.of(key, Handler.class, spec -> spec.every(Duration.ofMinutes(1)));
    }

    @Test
    void findJobReturnsEmptyForAnUnknownKey() {
        assertThat(mohs.findJob(JobKey.of("ghost"))).isEmpty();
    }

    /** ADR-0034: pendente cancela por CAS direto — o caminho da flag nem é tentado. */
    @Test
    void cancelOfAPendingExecutionCancelsDirectly() {
        ExecutionId id = ExecutionId.of("exec-1");
        Execution cancelled = new Execution(id, JobKey.of("welcome-email"), ExecutionState.CANCELLED, NOW, null, List.of(), "test");
        when(workQueue.cancelQueued(eq(id), any())).thenReturn(true);
        when(historyStore.find(eq(id), any())).thenReturn(Optional.of(cancelled));

        assertThat(mohs.cancel(id)).contains(cancelled);
        verify(leaseStore, never()).requestCancellation(any());
    }

    /** ADR-0034: o CAS perdeu (execução já roda) → cai no caminho da flag; o retorno é o estado corrente, não necessariamente terminal — o contrato do 202. */
    @Test
    void cancelOfARunningExecutionFallsThroughToTheFlag() {
        ExecutionId id = ExecutionId.of("exec-1");
        Execution running = new Execution(id, JobKey.of("welcome-email"), ExecutionState.RUNNING, NOW, null, List.of(), "test");
        when(workQueue.cancelQueued(eq(id), any())).thenReturn(false);
        when(leaseStore.requestCancellation(id)).thenReturn(true);
        when(historyStore.find(eq(id), any())).thenReturn(Optional.of(running));

        assertThat(mohs.cancel(id)).contains(running);
        verify(leaseStore).requestCancellation(id);
    }

    /** TOCTOU fechado (review do ciclo ADR-0034): RUNNING vira RETRY_WAITING entre o CAS e a flag — a segunda passada pega a transição e a ordem do operador não cai no vazio. */
    @Test
    void cancelRetriesThePairWhenAnAttemptCompletionRacesInBetween() {
        ExecutionId id = ExecutionId.of("exec-1");
        Execution cancelled = new Execution(id, JobKey.of("welcome-email"), ExecutionState.CANCELLED, NOW, null, List.of(), "test");
        when(workQueue.cancelQueued(eq(id), any())).thenReturn(false, true);
        when(leaseStore.requestCancellation(id)).thenReturn(false);
        when(historyStore.find(eq(id), any())).thenReturn(Optional.of(cancelled));

        assertThat(mohs.cancel(id)).contains(cancelled);
        verify(workQueue, times(2)).cancelQueued(eq(id), any());
    }

    @Test
    void cancelOfAnUnknownExecutionIsEmpty() {
        ExecutionId id = ExecutionId.of("ghost");
        when(workQueue.cancelQueued(eq(id), any())).thenReturn(false);
        when(leaseStore.requestCancellation(id)).thenReturn(false);
        when(historyStore.find(eq(id), any())).thenReturn(Optional.empty());

        assertThat(mohs.cancel(id)).isEmpty();
    }

    /** GET /nodes por baixo: heartbeat mais recente primeiro, empate por nodeId — ordem exposta em API é contrato, nunca a ordem física da tabela (ADR-0012). */
    @Test
    void nodesAreListedMostRecentHeartbeatFirstWithAStableTiebreak() {
        when(nodeStore.findAll()).thenReturn(List.of(
                new StoredNode("node-old", EngineState.RUNNING, NOW.minusSeconds(120), 1, NOW.minusSeconds(105)),
                new StoredNode("node-tie-b", EngineState.RUNNING, NOW.minusSeconds(30), 1, NOW.minusSeconds(15)),
                new StoredNode("node-fresh", EngineState.RUNNING, NOW, 1, NOW.plusSeconds(15)),
                new StoredNode("node-tie-a", EngineState.STOPPED, NOW.minusSeconds(30), 1, NOW.minusSeconds(30))));

        assertThat(mohs.nodes())
                .extracting(NodeSnapshot::nodeId)
                .containsExactly("node-fresh", "node-tie-a", "node-tie-b", "node-old");
    }

    /** ADR-0033/M3: retry manual — o CAS da porta arma com o "agora" do Clock injetado e o retorno é a execução já RETRY_WAITING. */
    @Test
    void retryOfAFailedExecutionRearmsThroughTheStore() {
        ExecutionId id = ExecutionId.of("exec-1");
        Execution rearmed = new Execution(id, JobKey.of("welcome-email"), ExecutionState.RETRY_WAITING, NOW, null, List.of(), "test");
        when(workQueue.rearmForManualRetry(id, NOW)).thenReturn(true);
        when(historyStore.find(eq(id), any())).thenReturn(Optional.of(rearmed));

        assertThat(mohs.retry(id)).contains(rearmed);
    }

    @Test
    void retryOfAnUnknownExecutionIsEmpty() {
        ExecutionId id = ExecutionId.of("ghost");
        when(workQueue.rearmForManualRetry(id, NOW)).thenReturn(false);
        when(historyStore.find(eq(id), any())).thenReturn(Optional.empty());

        assertThat(mohs.retry(id)).isEmpty();
    }

    /** CAS derrotado com a linha ainda FAILED = o guard de retired barrou — a mensagem aponta o job removido, não o estado. */
    @Test
    void retryOfARetiredJobsExecutionThrowsNamingTheRemovedJob() {
        ExecutionId id = ExecutionId.of("exec-1");
        Execution failed = new Execution(id, JobKey.of("welcome-email"), ExecutionState.FAILED, NOW, null, List.of(), "test");
        when(workQueue.rearmForManualRetry(id, NOW)).thenReturn(false);
        when(historyStore.find(eq(id), any())).thenReturn(Optional.of(failed));

        assertThatThrownBy(() -> mohs.retry(id))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("removed job");
    }

    /** POST duplicado (202 perdido, script reenviou): a execução já está rearmada — o 409 ensina a causa provável, não a lição errada. */
    @Test
    void retryOfAnAlreadyRearmedExecutionThrowsNamingTheDuplicate() {
        ExecutionId id = ExecutionId.of("exec-1");
        Execution rearmed = new Execution(id, JobKey.of("welcome-email"), ExecutionState.RETRY_WAITING, NOW, null, List.of(), "test");
        when(workQueue.rearmForManualRetry(id, NOW)).thenReturn(false);
        when(historyStore.find(eq(id), any())).thenReturn(Optional.of(rearmed));

        assertThatThrownBy(() -> mohs.retry(id))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("already queued to run again");
    }

    /** Estado ≠ FAILED: a exceção nomeia o estado atual — às 3h ninguém deveria adivinhar por que o retry "não pegou". */
    @Test
    void retryOfANonFailedExecutionThrowsNamingTheState() {
        ExecutionId id = ExecutionId.of("exec-1");
        Execution running = new Execution(id, JobKey.of("welcome-email"), ExecutionState.RUNNING, NOW, null, List.of(), "test");
        when(workQueue.rearmForManualRetry(id, NOW)).thenReturn(false);
        when(historyStore.find(eq(id), any())).thenReturn(Optional.of(running));

        assertThatThrownBy(() -> mohs.retry(id))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("RUNNING")
                .hasMessageContaining("only FAILED");
    }

    /** ADR-0035: ocorrência do scheduler cancelada ainda pendente não passa pelo caminho de conclusão que rearma — o cancel cura a corrente fixed-delay aqui. */
    @Test
    void cancelOfAPendingSchedulerOccurrenceRearmsTheAfterFinishChain() {
        JobStore jobStoreMock = mock(JobStore.class);
        MohsImpl mohsWithMockedJobStore = new MohsImpl(jobStoreMock, workQueue, historyStore, leaseStore, work -> work.run(), nodeStore, rateLimitStore, handlerRegistry,
                new MutableClock(NOW, ZoneId.of("UTC")), mock(MohsLifecycle.class), mock(BatchStore.class), new BatchCompletionCallbacks(), new RunnerRegistry(List.of(MohsRunner.io("io").build())), () -> { });
        ExecutionId id = ExecutionId.of("exec-1");
        JobDefinition afterFinish = JobDefinition.of("poll", Handler.class, spec -> spec.everyAfterFinish(Duration.ofMinutes(5)));
        when(jobStoreMock.find(JobKey.of("poll"))).thenReturn(Optional.of(new StoredJob(afterFinish, false, false, null)));
        when(workQueue.cancelQueued(eq(id), any())).thenReturn(true);
        when(historyStore.find(eq(id), any())).thenReturn(Optional.of(
                new Execution(id, JobKey.of("poll"), ExecutionState.CANCELLED, NOW, null, List.of(), "scheduler")));

        mohsWithMockedJobStore.cancel(id);

        verify(jobStoreMock).armNextFire(JobKey.of("poll"), NOW.plus(Duration.ofMinutes(5)));
    }

    /** Execução manual cancelada não é a corrente — nada a rearmar. */
    @Test
    void cancelOfAPendingManualExecutionDoesNotTouchTheChain() {
        JobStore jobStoreMock = mock(JobStore.class);
        MohsImpl mohsWithMockedJobStore = new MohsImpl(jobStoreMock, workQueue, historyStore, leaseStore, work -> work.run(), nodeStore, rateLimitStore, handlerRegistry,
                new MutableClock(NOW, ZoneId.of("UTC")), mock(MohsLifecycle.class), mock(BatchStore.class), new BatchCompletionCallbacks(), new RunnerRegistry(List.of(MohsRunner.io("io").build())), () -> { });
        ExecutionId id = ExecutionId.of("exec-1");
        when(workQueue.cancelQueued(eq(id), any())).thenReturn(true);
        when(historyStore.find(eq(id), any())).thenReturn(Optional.of(
                new Execution(id, JobKey.of("poll"), ExecutionState.CANCELLED, NOW, null, List.of(), "api:user")));

        mohsWithMockedJobStore.cancel(id);

        verify(jobStoreMock, never()).armNextFire(any(), any());
    }

    @Test
    void findJobReturnsTheStoredDefinition() {
        jobStore.upsert(onDemand("welcome-email"));

        Optional<JobSnapshot> snapshot = mohs.findJob(JobKey.of("welcome-email"));

        assertThat(snapshot).isPresent();
        assertThat(snapshot.get().definition().key()).isEqualTo(JobKey.of("welcome-email"));
        assertThat(snapshot.get().paused()).isFalse();
    }

    @Test
    void onDemandJobsNeverHaveANextFireAt() {
        jobStore.upsert(onDemand("welcome-email"));

        assertThat(mohs.findJob(JobKey.of("welcome-email")).orElseThrow().nextFireAt()).isNull();
    }

    /** ADR-0035: o snapshot lê o estado REAL do trigger (armado no upsert), não um recálculo por cima do relógio a cada leitura. */
    @Test
    void intervalJobsExposeTheStoredNextFireAt() {
        jobStore.upsert(everyMinute("digest"));

        Instant nextFireAt = mohs.findJob(JobKey.of("digest")).orElseThrow().nextFireAt();

        assertThat(nextFireAt).isEqualTo(NOW.plus(Duration.ofMinutes(1)));
    }

    @Test
    void pausedJobsNeverHaveANextFireAtEvenWithASchedule() {
        jobStore.upsert(everyMinute("digest"));
        jobStore.pause(JobKey.of("digest"));

        assertThat(mohs.findJob(JobKey.of("digest")).orElseThrow().nextFireAt()).isNull();
    }

    @Test
    void jobsListsEveryRegisteredDefinition() {
        jobStore.upsert(onDemand("welcome-email"));
        jobStore.upsert(everyMinute("digest"));

        assertThat(mohs.jobs()).extracting(s -> s.definition().key().value())
                .containsExactlyInAnyOrder("welcome-email", "digest");
    }

    /** ADR-0036: reschedule delega pra porta e devolve o snapshot já com a agenda nova e o trigger recomputado. */
    @Test
    void rescheduleReturnsTheFreshSnapshot() {
        jobStore.upsert(everyMinute("digest"));

        Optional<JobSnapshot> snapshot = mohs.reschedule(JobKey.of("digest"), new IntervalSpec(Duration.ofMinutes(5), false));

        assertThat(snapshot).isPresent();
        assertThat(snapshot.get().definition().schedule()).isEqualTo(new IntervalSpec(Duration.ofMinutes(5), false));
        assertThat(snapshot.get().nextFireAt()).isEqualTo(NOW.plus(Duration.ofMinutes(5)));
    }

    @Test
    void rescheduleOfAnUnknownJobIsEmpty() {
        assertThat(mohs.reschedule(JobKey.of("ghost"), new OnDemandSpec())).isEmpty();
    }

    @Test
    void pauseIsReflectedOnTheNextRead() {
        jobStore.upsert(onDemand("welcome-email"));

        mohs.pause(JobKey.of("welcome-email"));

        assertThat(mohs.findJob(JobKey.of("welcome-email")).orElseThrow().paused()).isTrue();
    }

    @Test
    void resumeIsReflectedOnTheNextRead() {
        jobStore.upsert(onDemand("welcome-email"));
        mohs.pause(JobKey.of("welcome-email"));

        mohs.resume(JobKey.of("welcome-email"));

        assertThat(mohs.findJob(JobKey.of("welcome-email")).orElseThrow().paused()).isFalse();
    }

    @Test
    void removeRetiresAProgrammaticDefinition() {
        jobStore.upsert(onDemand("welcome-email"));

        mohs.remove(JobKey.of("welcome-email"));

        assertThat(mohs.findJob(JobKey.of("welcome-email"))).isEmpty();
    }

    /** Aposentadoria de job anotado é remover a anotação (scanner marca ORPHANED) — o erro precisa ensinar isso, não só recusar. */
    @Test
    void removeRejectsAnAnnotationSourcedDefinitionWithAnErrorThatTeaches() {
        // JobDefinition.of hardcoda PROGRAMMATIC — só o construtor canônico produz ANNOTATION
        jobStore.upsert(new JobDefinition(JobKey.of("annotated"), null, Handler.class, new OnDemandSpec(),
                null, null, Misfire.IGNORE, true, 0, 0, null, null, DefinitionSource.ANNOTATION));

        assertThatThrownBy(() -> mohs.remove(JobKey.of("annotated")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("remove the annotation");
        assertThat(mohs.findJob(JobKey.of("annotated"))).isPresent();
    }

    /** Mesma postura de {@code pause}: job desconhecido é no-op, não erro. */
    @Test
    void removeOfAnUnknownJobIsANoOp() {
        assertThatCode(() -> mohs.remove(JobKey.of("ghost"))).doesNotThrowAnyException();
    }

    @Test
    void payloadTypeIsEmptyForAnUnregisteredHandler() {
        assertThat(mohs.payloadType(JobKey.of("ghost"))).isEmpty();
    }

    @Test
    void payloadTypeDelegatesToTheHandlerRegistry() {
        handlerRegistry.register(JobKey.of("welcome-email"), (payload, ctx) -> { }, String.class);

        assertThat(mohs.payloadType(JobKey.of("welcome-email"))).contains(String.class);
    }

    @Test
    void findExecutionDelegatesToTheExecutionStore() {
        Execution execution = new Execution(ExecutionId.of("exec-1"), JobKey.of("welcome-email"), ExecutionState.ENQUEUED, NOW, null, List.of(), "tester");
        when(historyStore.find(eq(ExecutionId.of("exec-1")), any())).thenReturn(Optional.of(execution));

        assertThat(mohs.findExecution(ExecutionId.of("exec-1"))).contains(execution);
    }

    @Test
    void executionsResolvesTheCursorStringIntoAnExecutionId() {
        when(historyStore.findPage(any(), any(), any(), any(), any(), eq(21), any())).thenReturn(List.of());

        mohs.executions(new ExecutionQuery(JobKey.of("welcome-email"), ExecutionState.ENQUEUED, null, null, "exec-9", 21));

        verify(historyStore).findPage(eq(JobKey.of("welcome-email")), eq(ExecutionState.ENQUEUED), eq(null), eq(null), eq(ExecutionId.of("exec-9")), eq(21), any());
    }

    @Test
    void executionsAllowsANullCursorForTheFirstPage() {
        when(historyStore.findPage(any(), any(), any(), any(), eq(null), eq(10), any())).thenReturn(List.of());

        mohs.executions(new ExecutionQuery(null, null, null, null, null, 10));

        verify(historyStore).findPage(eq(null), eq(null), eq(null), eq(null), eq(null), eq(10), any());
    }

    /** ?cursor= (em branco) na REST = primeira página — antes ExecutionId.of("") estourava IAE, que a borda respondia como 500. */
    @Test
    void executionsTreatsABlankCursorAsFirstPage() {
        when(historyStore.findPage(any(), any(), any(), any(), eq(null), eq(10), any())).thenReturn(List.of());

        mohs.executions(new ExecutionQuery(null, null, null, null, "", 10));

        verify(historyStore).findPage(eq(null), eq(null), eq(null), eq(null), eq(null), eq(10), any());
    }

    /** Item 49: janela inválida falha ANTES das duas queries — é o comportamento que justifica duplicar o predicado do record na fachada. */
    @Test
    void aNonPositiveWindowFailsBeforeTouchingTheStore() {
        assertThatThrownBy(() -> mohs.overview(Duration.ZERO)).isInstanceOf(IllegalArgumentException.class);

        verifyNoInteractions(historyStore);
    }

    /** GET /overview — composição pura: o `since` sai do Clock injetado (nunca do relógio da máquina) e a normalização (zeros dos estados vivos) é do OverviewSnapshot. */
    @Test
    void overviewComposesActiveCountsAndTheWindowedThroughput() {
        when(historyStore.countActiveByState(NOW)).thenReturn(Map.of(ExecutionState.ENQUEUED, 7L));
        when(historyStore.countTerminalOutcomesSince(NOW.minusSeconds(60)))
                .thenReturn(Map.of(ExecutionState.SUCCEEDED, 41L));

        OverviewSnapshot overview = mohs.overview(Duration.ofSeconds(60));

        assertThat(overview.executionCountsByState()).containsOnly(
                entry(ExecutionState.ENQUEUED, 7L),
                entry(ExecutionState.RUNNING, 0L),
                entry(ExecutionState.RETRY_WAITING, 0L));
        assertThat(overview.throughputWindow()).isEqualTo(Duration.ofSeconds(60));
        assertThat(overview.succeededInWindow()).isEqualTo(41L);
        assertThat(overview.failedInWindow()).isZero();
    }

    /**
     * O total nasce fechado: os membros são coletados antes de a linha do lote
     * existir, então o lote já sabe quantas conclusões o fecham (ADR-0043).
     * Cada execução sai carregando o batchId — sem isso a conclusão não teria
     * como contar o membro.
     */
    @Test
    void batchFixesTheTotalUpFrontAndStampsEveryMemberWithTheBatchId() {
        jobStore.upsert(onDemand("welcome-email"));
        JobRef<String> ref = JobRef.of("welcome-email", String.class);

        Batch batch = mohs.batch("nightly", members -> {
            members.add(ref, "ana");
            members.add(ref, "bob");
        });

        verify(batchStore).insert(batch.batchId(), 2);
        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<HistoryStore.NewExecution>> recorded = ArgumentCaptor.forClass(List.class);
        verify(historyStore, times(2)).record(recorded.capture());
        assertThat(recorded.getAllValues()).allSatisfy(records -> assertThat(records).singleElement().satisfies(execution ->
                assertThat(execution.correlationId()).isEqualTo(batch.batchId())));
    }

    /** Lote vazio nunca completaria — recusado na entrada, e nada é persistido. */
    @Test
    void anEmptyBatchIsRefusedBeforeAnythingIsWritten() {
        assertThatThrownBy(() -> mohs.batch("nightly", members -> { }))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at least one member");

        verifyNoInteractions(batchStore);
        verifyNoInteractions(historyStore);
        verifyNoInteractions(workQueue);
    }

    /**
     * Caracterização do comportamento atual: a definição de cada membro só é
     * checada DEPOIS de a linha do lote existir, então um membro sem job
     * registrado derruba a chamada com o lote — e os membros anteriores — já
     * persistidos. Fixado aqui como rede de proteção; mudar a ordem de
     * validação é decisão do time, não do refactor.
     */
    @Test
    void aMemberWithoutARegisteredJobFailsBeforeAnythingIsWritten() {
        jobStore.upsert(onDemand("welcome-email"));
        JobRef<String> known = JobRef.of("welcome-email", String.class);
        JobRef<String> unknown = JobRef.of("ghost", String.class);

        assertThatThrownBy(() -> mohs.batch("nightly", members -> {
            members.add(known, "ana");
            members.add(unknown, "bob");
        }))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("no job registered for id 'ghost'");

        verifyNoInteractions(batchStore);
        verifyNoInteractions(historyStore);
        verifyNoInteractions(workQueue);
    }

    /** A recusa tem que ensinar o que fazer, não só dizer não (ADR-0043, opção B). */
    @Test
    void retryingABatchMemberExplainsWhyAndWhatToDoInstead() {
        jobStore.upsert(onDemand("welcome-email"));
        Execution member = new Execution(ExecutionId.of("019abc-m"), JobKey.of("welcome-email"),
                ExecutionState.FAILED, NOW, null, List.of(), "application", Priority.NORMAL, null, "b9");
        when(workQueue.rearmForManualRetry(any(), any())).thenReturn(false);
        when(historyStore.find(eq(ExecutionId.of("019abc-m")), any())).thenReturn(Optional.of(member));

        assertThatThrownBy(() -> mohs.retry(ExecutionId.of("019abc-m")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("member of batch b9")
                .hasMessageContaining("Schedule the job standalone");
    }
}
