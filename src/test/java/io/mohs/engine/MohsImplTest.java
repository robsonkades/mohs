package io.mohs.engine;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.mohs.core.ExecutionQuery;
import io.mohs.core.JobSnapshot;
import io.mohs.core.Mohs;
import io.mohs.core.MohsLifecycle;
import io.mohs.core.definition.DefinitionSource;
import io.mohs.core.definition.JobDefinition;
import io.mohs.core.definition.JobSpec;
import io.mohs.core.execution.Execution;
import io.mohs.core.execution.ExecutionId;
import io.mohs.core.execution.ExecutionState;
import io.mohs.core.job.JobKey;
import io.mohs.core.schedule.IntervalSpec;
import io.mohs.core.schedule.Misfire;
import io.mohs.core.schedule.OnDemandSpec;
import io.mohs.test.InMemoryJobStore;
import io.mohs.test.MutableClock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Métodos de leitura de {@link MohsImpl} — a costura entre {@link Mohs}
 * e as portas {@link JobStore}/{@link ExecutionStore}/{@link
 * HandlerRegistry} que {@code io.mohs.rest} consome. {@link
 * InMemoryJobStore} real (comportamento de verdade, sem mock); {@link
 * ExecutionStore} mockado — a lógica de paginação/filtro dele já tem
 * suíte própria em {@code JdbcExecutionStoreTest}.
 */
class MohsImplTest {

    private static final Instant NOW = Instant.parse("2026-08-15T12:00:00Z");

    record Handler() {
    }

    private InMemoryJobStore jobStore;
    private ExecutionStore executionStore;
    private HandlerRegistry handlerRegistry;
    private MohsImpl mohs;

    @BeforeEach
    void setUp() {
        MutableClock clock = new MutableClock(NOW, ZoneId.of("UTC"));
        jobStore = new InMemoryJobStore(clock);
        executionStore = mock(ExecutionStore.class);
        handlerRegistry = new HandlerRegistry();
        mohs = new MohsImpl(jobStore, executionStore, handlerRegistry, clock, mock(MohsLifecycle.class));
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
        when(executionStore.cancelIfPending(id)).thenReturn(true);
        when(executionStore.find(id)).thenReturn(Optional.of(cancelled));

        assertThat(mohs.cancel(id)).contains(cancelled);
        verify(executionStore, never()).requestCancellation(any());
    }

    /** ADR-0034: o CAS perdeu (execução já roda) → cai no caminho da flag; o retorno é o estado corrente, não necessariamente terminal — o contrato do 202. */
    @Test
    void cancelOfARunningExecutionFallsThroughToTheFlag() {
        ExecutionId id = ExecutionId.of("exec-1");
        Execution running = new Execution(id, JobKey.of("welcome-email"), ExecutionState.RUNNING, NOW, null, List.of(), "test");
        when(executionStore.cancelIfPending(id)).thenReturn(false);
        when(executionStore.requestCancellation(id)).thenReturn(true);
        when(executionStore.find(id)).thenReturn(Optional.of(running));

        assertThat(mohs.cancel(id)).contains(running);
        verify(executionStore).requestCancellation(id);
    }

    /** TOCTOU fechado (review do ciclo ADR-0034): RUNNING vira RETRY_SCHEDULED entre o CAS e a flag — a segunda passada pega a transição e a ordem do operador não cai no vazio. */
    @Test
    void cancelRetriesThePairWhenAnAttemptCompletionRacesInBetween() {
        ExecutionId id = ExecutionId.of("exec-1");
        Execution cancelled = new Execution(id, JobKey.of("welcome-email"), ExecutionState.CANCELLED, NOW, null, List.of(), "test");
        when(executionStore.cancelIfPending(id)).thenReturn(false, true);
        when(executionStore.requestCancellation(id)).thenReturn(false);
        when(executionStore.find(id)).thenReturn(Optional.of(cancelled));

        assertThat(mohs.cancel(id)).contains(cancelled);
        verify(executionStore, times(2)).cancelIfPending(id);
    }

    @Test
    void cancelOfAnUnknownExecutionIsEmpty() {
        ExecutionId id = ExecutionId.of("ghost");
        when(executionStore.cancelIfPending(id)).thenReturn(false);
        when(executionStore.requestCancellation(id)).thenReturn(false);
        when(executionStore.find(id)).thenReturn(Optional.empty());

        assertThat(mohs.cancel(id)).isEmpty();
    }

    /** ADR-0035: ocorrência do scheduler cancelada ainda pendente não passa pelo caminho de conclusão que rearma — o cancel cura a corrente fixed-delay aqui. */
    @Test
    void cancelOfAPendingSchedulerOccurrenceRearmsTheAfterFinishChain() {
        JobStore jobStoreMock = mock(JobStore.class);
        MohsImpl mohsWithMockedJobStore = new MohsImpl(jobStoreMock, executionStore, handlerRegistry,
                new MutableClock(NOW, ZoneId.of("UTC")), mock(MohsLifecycle.class));
        ExecutionId id = ExecutionId.of("exec-1");
        JobDefinition afterFinish = JobDefinition.of("poll", Handler.class, spec -> spec.everyAfterFinish(Duration.ofMinutes(5)));
        when(jobStoreMock.find(JobKey.of("poll"))).thenReturn(Optional.of(new StoredJob(afterFinish, false, false, 0, null)));
        when(executionStore.cancelIfPending(id)).thenReturn(true);
        when(executionStore.find(id)).thenReturn(Optional.of(
                new Execution(id, JobKey.of("poll"), ExecutionState.CANCELLED, NOW, null, List.of(), "scheduler")));

        mohsWithMockedJobStore.cancel(id);

        verify(jobStoreMock).armNextFire(JobKey.of("poll"), NOW.plus(Duration.ofMinutes(5)));
    }

    /** Execução manual cancelada não é a corrente — nada a rearmar. */
    @Test
    void cancelOfAPendingManualExecutionDoesNotTouchTheChain() {
        JobStore jobStoreMock = mock(JobStore.class);
        MohsImpl mohsWithMockedJobStore = new MohsImpl(jobStoreMock, executionStore, handlerRegistry,
                new MutableClock(NOW, ZoneId.of("UTC")), mock(MohsLifecycle.class));
        ExecutionId id = ExecutionId.of("exec-1");
        when(executionStore.cancelIfPending(id)).thenReturn(true);
        when(executionStore.find(id)).thenReturn(Optional.of(
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
        when(executionStore.find(ExecutionId.of("exec-1"))).thenReturn(Optional.of(execution));

        assertThat(mohs.findExecution(ExecutionId.of("exec-1"))).contains(execution);
    }

    @Test
    void executionsResolvesTheCursorStringIntoAnExecutionId() {
        when(executionStore.findPage(any(), any(), any(), any(), any(), eq(21))).thenReturn(List.of());

        mohs.executions(new ExecutionQuery(JobKey.of("welcome-email"), ExecutionState.ENQUEUED, null, null, "exec-9", 21));

        verify(executionStore).findPage(JobKey.of("welcome-email"), ExecutionState.ENQUEUED, null, null, ExecutionId.of("exec-9"), 21);
    }

    @Test
    void executionsAllowsANullCursorForTheFirstPage() {
        when(executionStore.findPage(any(), any(), any(), any(), eq(null), eq(10))).thenReturn(List.of());

        mohs.executions(new ExecutionQuery(null, null, null, null, null, 10));

        verify(executionStore).findPage(null, null, null, null, null, 10);
    }

    /** ?cursor= (em branco) na REST = primeira página — antes ExecutionId.of("") estourava IAE, que a borda respondia como 500. */
    @Test
    void executionsTreatsABlankCursorAsFirstPage() {
        when(executionStore.findPage(any(), any(), any(), any(), eq(null), eq(10))).thenReturn(List.of());

        mohs.executions(new ExecutionQuery(null, null, null, null, "", 10));

        verify(executionStore).findPage(null, null, null, null, null, 10);
    }
}
