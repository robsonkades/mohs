/*
 * Copyright 2026 The Mohs Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.mohs.engine;

import java.time.Clock;
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
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Read methods of {@link MohsImpl} — the seam between {@link Mohs} and the
 * {@link JobStore}/{@link WorkQueue}/{@link HistoryStore}/{@link LeaseStore} ports that
 * {@code io.mohs.rest} consumes.
 *
 * <p>{@link InMemoryJobStore} is real here, so the store's behaviour is exercised rather than
 * stubbed; the remaining ports are mocked, because their queueing and derivation logic already has
 * a suite of its own in the {@code Jdbc*Test} classes under {@code io.mohs.store.jdbc}.
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
        mohs = new MohsImpl(jobStore, workQueue, historyStore, leaseStore, (work, onDurable) -> { work.run(); onDurable.run(); }, nodeStore, rateLimitStore, handlerRegistry, clock, mock(MohsLifecycle.class), batchStore, new BatchCompletionCallbacks(), new RunnerRegistry(List.of(MohsRunner.io("io").build())), () -> { }, List.of(), Runnable::run);
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

    /** A pending execution is cancelled by a direct CAS; the cooperative-flag path is not even attempted. */
    @Test
    void cancelOfAPendingExecutionCancelsDirectly() {
        ExecutionId id = ExecutionId.of("exec-1");
        Execution cancelled = new Execution(id, JobKey.of("welcome-email"), ExecutionState.CANCELLED, NOW, null, List.of(), "test");
        when(workQueue.cancelQueued(eq(id), any())).thenReturn(true);
        when(historyStore.find(eq(id), any())).thenReturn(Optional.of(cancelled));

        assertThat(mohs.cancel(id)).contains(cancelled);
        verify(leaseStore, never()).requestCancellation(any());
    }

    /**
     * The CAS lost because the execution is already running, so cancellation falls back to raising
     * the cooperative flag. The return value is the CURRENT state, not necessarily a terminal one —
     * which is exactly what the 202 promises: the request was accepted, not that it finished.
     */
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

    /**
     * Closes a time-of-check/time-of-use window: the execution moves from RUNNING to RETRY_WAITING
     * between the CAS and the flag. The second pass catches that transition, so the operator's
     * cancel does not fall into the gap and vanish.
     */
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

    /**
     * What backs {@code GET /nodes}: most recent heartbeat first, ties broken by nodeId. Order
     * exposed through an API is part of the contract — never whatever physical order the table
     * happens to return.
     */
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

    /**
     * Manual retry: the port's CAS arms the attempt from the injected clock's "now", and the call
     * returns the execution already in RETRY_WAITING.
     */
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

    /**
     * A defeated CAS with the row still FAILED means the retired-job guard blocked it, so the
     * message must point at the removed job rather than at the state, which is a red herring here.
     */
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

    /**
     * A duplicate POST — the 202 was lost and a script resent it. The execution is already rearmed,
     * so the 409 has to name the likely cause instead of teaching the wrong lesson.
     */
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

    /** State other than FAILED: the exception names the current state, so nobody has to guess at 3 a.m. why the retry "did not take". */
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

    /**
     * A cancelled scheduler occurrence that is still pending never reaches the completion path that
     * rearms the trigger, so cancellation is what keeps a fixed-delay chain from stalling forever.
     */
    @Test
    void cancelOfAPendingSchedulerOccurrenceRearmsTheAfterFinishChain() {
        JobStore jobStoreMock = mock(JobStore.class);
        MohsImpl mohsWithMockedJobStore = new MohsImpl(jobStoreMock, workQueue, historyStore, leaseStore, (work, onDurable) -> { work.run(); onDurable.run(); }, nodeStore, rateLimitStore, handlerRegistry,
                new MutableClock(NOW, ZoneId.of("UTC")), mock(MohsLifecycle.class), mock(BatchStore.class), new BatchCompletionCallbacks(), new RunnerRegistry(List.of(MohsRunner.io("io").build())), () -> { }, List.of(), Runnable::run);
        ExecutionId id = ExecutionId.of("exec-1");
        JobDefinition afterFinish = JobDefinition.of("poll", Handler.class, spec -> spec.everyAfterFinish(Duration.ofMinutes(5)));
        when(jobStoreMock.find(JobKey.of("poll"))).thenReturn(Optional.of(new StoredJob(afterFinish, false, false, null)));
        when(workQueue.cancelQueued(eq(id), any())).thenReturn(true);
        when(historyStore.find(eq(id), any())).thenReturn(Optional.of(
                new Execution(id, JobKey.of("poll"), ExecutionState.CANCELLED, NOW, null, List.of(), "scheduler")));

        mohsWithMockedJobStore.cancel(id);

        verify(jobStoreMock).armNextFire(JobKey.of("poll"), NOW.plus(Duration.ofMinutes(5)));
    }

    /** A cancelled manual execution is not part of the recurring chain, so there is nothing to rearm. */
    @Test
    void cancelOfAPendingManualExecutionDoesNotTouchTheChain() {
        JobStore jobStoreMock = mock(JobStore.class);
        MohsImpl mohsWithMockedJobStore = new MohsImpl(jobStoreMock, workQueue, historyStore, leaseStore, (work, onDurable) -> { work.run(); onDurable.run(); }, nodeStore, rateLimitStore, handlerRegistry,
                new MutableClock(NOW, ZoneId.of("UTC")), mock(MohsLifecycle.class), mock(BatchStore.class), new BatchCompletionCallbacks(), new RunnerRegistry(List.of(MohsRunner.io("io").build())), () -> { }, List.of(), Runnable::run);
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

    /** The snapshot reads the trigger's REAL stored state, armed at upsert — not a fresh recomputation against the clock on every read. */
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

    /** Reschedule delegates to the port and returns the snapshot already carrying the new schedule and the recomputed trigger. */
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

    /**
     * Retiring an annotated job means removing the annotation, which the scanner then marks
     * ORPHANED. The error has to teach that, not merely refuse.
     */
    @Test
    void removeRejectsAnAnnotationSourcedDefinitionWithAnErrorThatTeaches() {
        // The public builder always stamps PROGRAMMATIC; only the canonical constructor yields ANNOTATION
        jobStore.upsert(new JobDefinition(JobKey.of("annotated"), null, Handler.class, new OnDemandSpec(),
                null, null, null, Misfire.IGNORE, false, true, 0, 0, null, null, DefinitionSource.ANNOTATION));

        assertThatThrownBy(() -> mohs.remove(JobKey.of("annotated")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("remove the annotation");
        assertThat(mohs.findJob(JobKey.of("annotated"))).isPresent();
    }

    /** Same stance as {@code pause}: an unknown job is a no-op, not an error. */
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

    /**
     * A blank {@code ?cursor=} from REST means the first page. Previously {@code ExecutionId.of("")}
     * threw IllegalArgumentException, which the edge turned into a 500.
     */
    @Test
    void executionsTreatsABlankCursorAsFirstPage() {
        when(historyStore.findPage(any(), any(), any(), any(), eq(null), eq(10), any())).thenReturn(List.of());

        mohs.executions(new ExecutionQuery(null, null, null, null, "", 10));

        verify(historyStore).findPage(eq(null), eq(null), eq(null), eq(null), eq(null), eq(10), any());
    }

    /** An invalid window fails BEFORE either query runs — the behaviour that justifies repeating the record's predicate in the facade. */
    @Test
    void aNonPositiveWindowFailsBeforeTouchingTheStore() {
        assertThatThrownBy(() -> mohs.overview(Duration.ZERO)).isInstanceOf(IllegalArgumentException.class);

        verifyNoInteractions(historyStore);
    }

    /**
     * {@code GET /overview} is pure composition: {@code since} comes from the injected clock, never
     * from the machine's, and normalising the live states to zero belongs to {@code OverviewSnapshot}.
     */
    @Test
    void overviewComposesActiveCountsAndTheWindowedThroughput() {
        when(historyStore.countActiveByState(NOW)).thenReturn(Map.of(ExecutionState.ENQUEUED, 7L));
        when(historyStore.countTerminalOutcomesSince(NOW.minusSeconds(60)))
                .thenReturn(Map.of(ExecutionState.SUCCEEDED, 41L));
        when(historyStore.countTerminalOutcomesSince(NOW.minusSeconds(10)))
                .thenReturn(Map.of(ExecutionState.SUCCEEDED, 9L, ExecutionState.FAILED, 1L));

        OverviewSnapshot overview = mohs.overview(Duration.ofSeconds(60));

        assertThat(overview.executionCountsByState()).containsOnly(
                entry(ExecutionState.ENQUEUED, 7L),
                entry(ExecutionState.RUNNING, 0L),
                entry(ExecutionState.RETRY_WAITING, 0L));
        assertThat(overview.throughput().window()).isEqualTo(Duration.ofSeconds(60));
        assertThat(overview.throughput().succeeded()).isEqualTo(41L);
        assertThat(overview.throughput().failed()).isZero();
    }

    /**
     * All three readings are taken from the SAME instant.
     *
     * <p>If the clock were re-read between them, the short window would end after the long one and
     * the rate would cover an interval other than the one it declares. A {@code Clock} that MOVES
     * on every read is what makes the regression detectable: the neighbouring tests use a stopped
     * clock, so a version that re-read {@code clock.instant()} would pass those without a single
     * assertion changing.
     */
    @Test
    void allThreeCountsDeriveFromASingleClockRead() {
        Clock advancing = mock(Clock.class);
        when(advancing.instant()).thenReturn(NOW, NOW.plusSeconds(5), NOW.plusSeconds(10));
        MohsImpl mohsWithAdvancingClock = new MohsImpl(jobStore, workQueue, historyStore, leaseStore,
                (work, onDurable) -> { work.run(); onDurable.run(); }, nodeStore, rateLimitStore, handlerRegistry, advancing,
                mock(MohsLifecycle.class), mock(BatchStore.class), new BatchCompletionCallbacks(),
                new RunnerRegistry(List.of(MohsRunner.io("io").build())), () -> { }, List.of(), Runnable::run);
        when(historyStore.countActiveByState(NOW)).thenReturn(Map.of());
        when(historyStore.countTerminalOutcomesSince(NOW.minusSeconds(60)))
                .thenReturn(Map.of(ExecutionState.SUCCEEDED, 600L));
        when(historyStore.countTerminalOutcomesSince(NOW.minusSeconds(10)))
                .thenReturn(Map.of(ExecutionState.SUCCEEDED, 50L));

        OverviewSnapshot overview = mohsWithAdvancingClock.overview(Duration.ofSeconds(60));

        assertThat(overview.recent().perSecond())
                .as("a re-read clock would make the short window's stub not match, and the rate would drop to zero")
                .isEqualTo(5.0);
        verify(advancing, times(1)).instant();
    }

    /**
     * The short reading comes from a window of its OWN, never sliced out of the long one, because
     * it is what yields the rate.
     *
     * <p>The live counts are instantaneous gauges, and by Little's Law they sit at zero for any
     * fast job — so without this reading the dashboard cannot tell "idle" from "working quickly".
     */
    @Test
    void overviewCarriesAShortReadingThatYieldsARate() {
        when(historyStore.countActiveByState(NOW)).thenReturn(Map.of());
        when(historyStore.countTerminalOutcomesSince(NOW.minusSeconds(60)))
                .thenReturn(Map.of(ExecutionState.SUCCEEDED, 600L));
        when(historyStore.countTerminalOutcomesSince(NOW.minusSeconds(10)))
                .thenReturn(Map.of(ExecutionState.SUCCEEDED, 47L, ExecutionState.FAILED, 3L));

        OverviewSnapshot overview = mohs.overview(Duration.ofSeconds(60));

        assertThat(overview.recent().window()).isEqualTo(Duration.ofSeconds(10));
        assertThat(overview.recent().perSecond())
                .as("50 terminal executions in 10s is 5/s — the number the activity panel needs")
                .isEqualTo(5.0);
    }

    /**
     * The total is fixed at birth: members are collected before the batch row exists, so the
     * batch already knows how many completions will close it.
     *
     * <p>Each execution carries the batchId out with it — without that, a completion would have no
     * way to count itself against the batch.
     */
    @Test
    void batchFixesTheTotalUpFrontAndStampsEveryMemberWithTheBatchId() {
        jobStore.upsert(onDemand("welcome-email"));
        JobRef<String> ref = JobRef.of("welcome-email", String.class);

        Batch batch = mohs.batch("nightly", members -> {
            members.add(ref, "ana");
            members.add(ref, "bob");
        });

        verify(batchStore).insert(batch.batchId(), "nightly", 2);
        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<HistoryStore.NewExecution>> recorded = ArgumentCaptor.forClass(List.class);
        verify(historyStore, times(2)).record(recorded.capture());
        assertThat(recorded.getAllValues()).allSatisfy(records -> assertThat(records).singleElement().satisfies(execution ->
                assertThat(execution.correlationId()).isEqualTo(batch.batchId())));
    }

    /** An empty batch could never complete, so it is refused at the door and nothing is persisted. */
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
     * Characterisation of the CURRENT behaviour, not an endorsement of it: each member's
     * definition is only checked AFTER the batch row exists, so a member with no registered job
     * fails the call with the batch — and the members before it — already persisted.
     *
     * <p>Pinned here as a safety net. Changing the validation order is a team decision, not
     * something a refactor should do on its way past.
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

    /** The refusal has to say what to do instead, not merely say no. */
    @Test
    void retryingABatchMemberExplainsWhyAndWhatToDoInstead() {
        jobStore.upsert(onDemand("welcome-email"));
        Execution member = new Execution(ExecutionId.of("019abc-m"), JobKey.of("welcome-email"),
                ExecutionState.FAILED, NOW, null, List.of(), "application", Priority.NORMAL, null, "b9", null);
        when(workQueue.rearmForManualRetry(any(), any())).thenReturn(false);
        when(historyStore.find(eq(ExecutionId.of("019abc-m")), any())).thenReturn(Optional.of(member));

        assertThatThrownBy(() -> mohs.retry(ExecutionId.of("019abc-m")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("member of batch b9")
                .hasMessageContaining("Schedule the job standalone");
    }
    /**
     * A blank name used to cross the write barrier, become durable, and only blow up on READ.
     *
     * <p>{@code BatchCompleted}'s constructor threw inside the event channel, where exceptions are
     * swallowed by design, so the user's {@code onCompletion} never ran and the batch turned into
     * a permanent 500 on {@code GET /batches/&#123;id&#125;}. Validating in the producer is what
     * closes that path.
     */
    @Test
    void aBlankBatchNameIsRejectedBeforeAnythingIsWritten() {
        jobStore.upsert(onDemand("welcome-email"));
        JobRef<String> ref = JobRef.of("welcome-email", String.class);

        assertThatThrownBy(() -> mohs.batch("  ", members -> members.add(ref, "ana")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must not be blank");

        verifyNoInteractions(batchStore);
    }
}
