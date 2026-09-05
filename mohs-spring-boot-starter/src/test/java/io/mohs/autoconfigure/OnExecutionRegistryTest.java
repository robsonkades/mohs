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
package io.mohs.autoconfigure;

import java.lang.reflect.Method;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.mohs.core.event.AttemptFailed;
import io.mohs.core.event.BatchCompleted;
import io.mohs.core.event.Cancelled;
import io.mohs.core.event.Enqueued;
import io.mohs.core.event.ExecutionEvent;
import io.mohs.core.event.ExecutionEventType;
import io.mohs.core.event.Failed;
import io.mohs.core.event.OnExecution;
import io.mohs.core.event.RetryScheduled;
import io.mohs.core.event.Started;
import io.mohs.core.event.Succeeded;
import io.mohs.core.execution.ExecutionId;
import io.mohs.core.job.JobKey;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The fan-out behind {@code @OnExecution}: every variant of the sealed {@link ExecutionEvent} reaches
 * the method declared for it, the job filter and the parameter shape are honoured, and one throwing
 * method never silences its neighbours.
 */
class OnExecutionRegistryTest {

    private static final ExecutionId ID = ExecutionId.of("exec-1");
    private static final JobKey WELCOME = JobKey.of("welcome-email");
    private static final Instant NOW = Instant.parse("2026-08-15T12:00:00Z");

    private final OnExecutionRegistry registry = new OnExecutionRegistry();

    /** One method per event type, all unfiltered — the routing table under test. */
    static class EveryEventObserver {
        final List<String> seen = new ArrayList<>();

        @OnExecution(event = ExecutionEventType.ENQUEUED)
        void enqueued(Enqueued e) {
            seen.add("enqueued:" + e.executionId().value());
        }

        @OnExecution(event = ExecutionEventType.STARTED)
        void started(Started e) {
            seen.add("started:" + e.attempt());
        }

        @OnExecution(event = ExecutionEventType.ATTEMPT_FAILED)
        void attemptFailed(AttemptFailed e) {
            seen.add("attemptFailed:" + e.error().getMessage());
        }

        @OnExecution(event = ExecutionEventType.RETRY_SCHEDULED)
        void retryScheduled(RetryScheduled e) {
            seen.add("retryScheduled:" + e.nextAttempt());
        }

        @OnExecution(event = ExecutionEventType.SUCCEEDED)
        void succeeded(Succeeded e) {
            seen.add("succeeded:" + e.attempt());
        }

        @OnExecution(event = ExecutionEventType.FAILED)
        void failed(Failed e) {
            seen.add("failed:" + e.attemptsExhausted());
        }

        @OnExecution(event = ExecutionEventType.CANCELLED)
        void cancelled(Cancelled e) {
            seen.add("cancelled:" + e.attempt());
        }

        @OnExecution(event = ExecutionEventType.BATCH_COMPLETED)
        void batchCompleted(BatchCompleted e) {
            seen.add("batch:" + e.batchId());
        }
    }

    static class FilteredObserver {
        final List<String> seen = new ArrayList<>();

        @OnExecution(event = ExecutionEventType.SUCCEEDED, job = "welcome-email")
        void onWelcome(Succeeded e) {
            seen.add("welcome:" + e.executionId().value());
        }

        @OnExecution(event = ExecutionEventType.SUCCEEDED, job = "other-job")
        void onOther() {
            seen.add("other");
        }

        @OnExecution(event = ExecutionEventType.SUCCEEDED)
        void withoutArguments() {
            seen.add("no-args");
        }

        @OnExecution(event = ExecutionEventType.SUCCEEDED)
        void broken(Succeeded e) {
            throw new IllegalStateException("observer broke");
        }
    }

    /** Both shapes on the same event — the filtered one only {@code registerAll} can build, since {@code validate} rejects it. */
    static class BatchObserver {
        final List<String> seen = new ArrayList<>();

        @OnExecution(event = ExecutionEventType.BATCH_COMPLETED)
        void anyBatch(BatchCompleted e) {
            seen.add("any:" + e.batchId());
        }

        @OnExecution(event = ExecutionEventType.BATCH_COMPLETED, job = "welcome-email")
        void filteredBatch(BatchCompleted e) {
            seen.add("filtered:" + e.batchId());
        }
    }

    static class MisdeclaredObserver {
        @OnExecution(event = ExecutionEventType.BATCH_COMPLETED, job = "welcome-email")
        void batchFilteredByJob(BatchCompleted e) {
        }

        @OnExecution(event = ExecutionEventType.SUCCEEDED)
        void twoParameters(Succeeded e, String extra) {
        }

        @OnExecution(event = ExecutionEventType.SUCCEEDED)
        void wrongParameterType(Failed e) {
        }
    }

    /**
     * Registered in name order because {@code getDeclaredMethods()} promises none: the isolation of
     * a throwing method is only exercised when it fires BEFORE its neighbours, and
     * {@code FilteredObserver#broken} sorts first.
     */
    private void registerAll(Object observer) {
        Method[] methods = observer.getClass().getDeclaredMethods();
        Arrays.sort(methods, Comparator.comparing(Method::getName));
        for (Method method : methods) {
            OnExecution annotation = method.getAnnotation(OnExecution.class);
            if (annotation != null) {
                registry.register(annotation, observer, method, observer.getClass().getSimpleName() + "#" + method.getName());
            }
        }
    }

    private static void validate(Method method) {
        OnExecutionRegistry.validate(method.getAnnotation(OnExecution.class), method, method.getName());
    }

    @Test
    void everyEventVariantReachesTheMethodDeclaredForIt() {
        EveryEventObserver observer = new EveryEventObserver();
        registerAll(observer);

        registry.on(new Enqueued(ID, WELCOME, NOW, "tester"));
        registry.on(new Started(ID, WELCOME, 1, NOW));
        registry.on(new AttemptFailed(ID, WELCOME, 1, new IllegalStateException("boom")));
        registry.on(new RetryScheduled(ID, WELCOME, 2, NOW.plusSeconds(30)));
        registry.on(new Succeeded(ID, WELCOME, 2));
        registry.on(new Failed(ID, WELCOME, 3, new IllegalStateException("gone"), true));
        registry.on(new Cancelled(ID, WELCOME, 1));
        registry.on(new BatchCompleted("b1", "nightly", 1, 1, 0));

        assertThat(observer.seen).containsExactly("enqueued:exec-1", "started:1", "attemptFailed:boom",
                "retryScheduled:2", "succeeded:2", "failed:true", "cancelled:1", "batch:b1");
    }

    /** The job filter, the no-argument shape, and isolation of a throwing method — in one fan-out. */
    @Test
    void theJobFilterAndTheParameterShapeAreHonouredAndAThrowingMethodIsIsolated() {
        FilteredObserver observer = new FilteredObserver();
        registerAll(observer);

        registry.on(new Succeeded(ID, WELCOME, 1));

        // Registration order is by name (broken, onOther, onWelcome, withoutArguments), so the
        // delivery order is deterministic too
        assertThat(observer.seen).containsExactly("welcome:exec-1", "no-args");
    }

    /** A batch belongs to no job: a subscription filtered by job never matches it, the unfiltered one does. */
    @Test
    void aBatchEventReachesOnlyTheUnfilteredSubscription() {
        BatchObserver observer = new BatchObserver();
        registerAll(observer);

        registry.on(new BatchCompleted("b1", "nightly", 1, 1, 0));

        assertThat(observer.seen).containsExactly("any:b1");
    }

    @Test
    void validationRejectsTheSubscriptionsThatWouldNeverFireOrCouldNotReceiveTheEvent() throws NoSuchMethodException {
        Method filteredBatch = MisdeclaredObserver.class.getDeclaredMethod("batchFilteredByJob", BatchCompleted.class);
        Method twoParameters = MisdeclaredObserver.class.getDeclaredMethod("twoParameters", Succeeded.class, String.class);
        Method wrongType = MisdeclaredObserver.class.getDeclaredMethod("wrongParameterType", Failed.class);

        assertThatThrownBy(() -> validate(filteredBatch))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("drop the job filter");
        assertThatThrownBy(() -> validate(twoParameters))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("2 parameters");
        assertThatThrownBy(() -> validate(wrongType))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("cannot receive it");
    }
}
