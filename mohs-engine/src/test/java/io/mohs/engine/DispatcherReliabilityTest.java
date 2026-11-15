/* Copyright 2026 The Mohs Authors. Licensed under the Apache License, Version 2.0. */
package io.mohs.engine;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.mohs.core.definition.JobDefinition;
import io.mohs.core.definition.JobOptions;
import io.mohs.core.event.ExecutionEvent;
import io.mohs.core.event.Failed;
import io.mohs.core.event.Started;
import io.mohs.core.execution.Execution;
import io.mohs.core.execution.ExecutionId;
import io.mohs.core.execution.ExecutionState;
import io.mohs.core.execution.Priority;
import io.mohs.core.job.JobKey;
import io.mohs.core.schedule.Misfire;
import io.mohs.core.schedule.OnDemandSpec;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

class DispatcherReliabilityTest {
    private static final Instant NOW = Instant.parse("2026-09-19T12:00:00Z");
    private static final String NODE = "01991337-0000-7000-8000-000000000001";
    private static final String TOKEN = "01991337-0000-7000-8000-000000000002";
    private static final ExecutionId ID = ExecutionId.of("01991337-0000-7000-8000-000000000003");
    private static final JobKey EXECUTABLE = JobKey.of("mail");
    private final LeaseStore leases = mock(LeaseStore.class);
    private final HandlerRegistry handlers = new HandlerRegistry();
    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    private final List<ExecutionEvent> events = new ArrayList<>();
    private final AtomicReference<LeaseStore.CompletionResult> completed = new AtomicReference<>();

    @BeforeEach
    void acceptCompletion() {
        doAnswer(call -> {
            List<LeaseStore.CompletionResult> results = call.getArgument(0);
            var result = results.getFirst();
            completed.set(result);
            return Map.of(result.executionId(), LeaseStore.Completion.owned(result, null));
        }).when(leases).complete(any());
    }

    private Dispatcher dispatcher() {
        return new Dispatcher(leases, mock(JobStore.class), handlers, Clock.fixed(NOW, ZoneOffset.UTC),
                List.of(), List.of(events::add), Runnable::run, new EngineMetrics(registry));
    }

    private JobDefinition definition(String key) {
        return JobDefinition.of(key, new OnDemandSpec(), JobOptions.defaults().withRetries(0), Misfire.IGNORE, false)
                .withExecutableKey(EXECUTABLE).withInputContract(EXECUTABLE.value());
    }

    private void dispatch(Dispatcher dispatcher, String key, CancellationSignal signal) {
        var execution = Execution.enqueued(ID, JobKey.of(key), NOW, "test", Priority.NORMAL);
        dispatcher.dispatch(execution, definition(key), new JobPayload("\"hello\"", EXECUTABLE.value()),
                signal, new Dispatcher.Grant(NODE, TOKEN, 1, NOW));
    }

    @Test
    void applicationAssertionErrorIsPersistedAsAFailedAttempt() {
        AssertionError failure = new AssertionError("handler failed after starting");
        handlers.register(EXECUTABLE, (_, _) -> { throw failure; }, String.class);

        dispatch(dispatcher(), "mail", new CancellationSignal());

        assertThat(completed.get().outcome()).isEqualTo(ExecutionState.FAILED);
        assertThat(completed.get().retryAt()).isNull();
        assertThat(completed.get().errorType()).isEqualTo(AssertionError.class.getName());
        assertThat(events).filteredOn(Failed.class::isInstance).singleElement()
                .satisfies(event -> assertThat(((Failed) event).error()).isSameAs(failure));
    }

    @Test
    void linkageErrorIsPersistedWithoutBypassingRetryBudget() {
        handlers.register(EXECUTABLE, (_, _) -> { throw new NoClassDefFoundError("dependency"); }, String.class);
        dispatch(dispatcher(), "mail", new CancellationSignal());
        assertThat(completed.get().outcome()).isEqualTo(ExecutionState.FAILED);
        assertThat(completed.get().retryAt()).isNull();
        assertThat(completed.get().errorType()).isEqualTo(NoClassDefFoundError.class.getName());
    }

    @ParameterizedTest
    @org.junit.jupiter.params.provider.MethodSource("nonFatalErrors")
    void otherNonFatalErrorsConsumeTheAttemptBudget(Error error) {
        handlers.register(EXECUTABLE, (_, _) -> { throw error; }, String.class);
        dispatch(dispatcher(), "mail", new CancellationSignal());
        assertThat(completed.get().outcome()).isEqualTo(ExecutionState.FAILED);
        assertThat(completed.get().retryAt()).isNull();
        assertThat(completed.get().errorType()).isEqualTo(error.getClass().getName());
    }

    static List<Error> nonFatalErrors() {
        return List.of(new java.io.IOError(new java.io.IOException("resource failure")),
                new java.util.ServiceConfigurationError("provider failure"), new Error("application failure"));
    }

    @Test
    void fatalVmErrorEscapesWithoutBeingReclassifiedAsAnApplicationFailure() {
        OutOfMemoryError fatal = new OutOfMemoryError("synthetic; no allocation pressure");
        handlers.register(EXECUTABLE, (_, _) -> { throw fatal; }, String.class);
        assertThatThrownBy(() -> dispatch(dispatcher(), "mail", new CancellationSignal())).isSameAs(fatal);
        assertThat(completed.get()).isNull();
        assertThat(Thread.currentThread().isInterrupted()).isFalse();
    }

    @ParameterizedTest
    @EnumSource(value = CancellationSignal.Reason.class, names = {"MANUAL", "SHUTDOWN"})
    void cancellationDuringDecodePreventsHandlerAndStartedEvent(CancellationSignal.Reason reason) {
        CancellationSignal signal = new CancellationSignal();
        HandlerRegistry gated = mock(HandlerRegistry.class);
        JobHandler handler = mock(JobHandler.class);
        doAnswer(_ -> java.util.Optional.of(handler)).when(gated).find(EXECUTABLE);
        doAnswer(_ -> {
            signal.requestCancellation(reason, reason == CancellationSignal.Reason.SHUTDOWN);
            return "hello";
        }).when(gated).decode(any(), any());
        Dispatcher dispatcher = new Dispatcher(leases, mock(JobStore.class), gated, Clock.fixed(NOW, ZoneOffset.UTC),
                List.of(), List.of(events::add), Runnable::run, new EngineMetrics(registry));

        dispatch(dispatcher, "mail", signal);

        verifyNoInteractions(handler);
        assertThat(events).noneMatch(Started.class::isInstance);
        assertThat(completed.get().outcome()).isEqualTo(reason == CancellationSignal.Reason.MANUAL
                ? ExecutionState.CANCELLED : ExecutionState.FAILED);
        assertThat(Thread.currentThread().isInterrupted()).isFalse();
    }

    @Test
    void dynamicSchedulesShareTheExecutableMetricSeries() {
        handlers.register(EXECUTABLE, (_, _) -> {}, String.class);
        Dispatcher dispatcher = dispatcher();
        for (int i = 0; i < 100; i++) dispatch(dispatcher, "customer-" + i, new CancellationSignal());

        assertThat(registry.get("mohs.dispatch.latency").tag("job", "mail").timer().count()).isEqualTo(100);
        assertThat(registry.get("mohs.execution.total").tag("job", "mail").tag("outcome", "succeeded")
                .counter().count()).isEqualTo(100);
        assertThat(registry.getMeters()).allSatisfy(meter -> {
            String job = meter.getId().getTag("job");
            assertThat(job == null || job.equals("mail")).isTrue();
        });
    }
}
