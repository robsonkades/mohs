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
        jobStore = new InMemoryJobStore();
        executionStore = mock(ExecutionStore.class);
        handlerRegistry = new HandlerRegistry();
        MutableClock clock = new MutableClock(NOW, ZoneId.of("UTC"));
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

    @Test
    void intervalJobsComputeNextFireAtFromNow() {
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
