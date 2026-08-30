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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mohs.core.event.AttemptFailed;
import io.mohs.core.event.BatchCompleted;
import io.mohs.core.event.Cancelled;
import io.mohs.core.event.Enqueued;
import io.mohs.core.event.ExecutionEvent;
import io.mohs.core.event.ExecutionEventType;
import io.mohs.core.event.ExecutionListener;
import io.mohs.core.event.Failed;
import io.mohs.core.event.OnExecution;
import io.mohs.core.event.RetryScheduled;
import io.mohs.core.event.Started;
import io.mohs.core.event.Succeeded;

/**
 * The one {@link ExecutionListener} behind every {@link OnExecution} method — the Observer the
 * engine sees, fanning out to the annotated methods {@link MohsJobScanner} found.
 *
 * <p>A single listener rather than one per method, for the same reason the event executor exists:
 * the engine publishes to each listener on its own task, so N annotated methods would become N
 * tasks per event, all repeating the same routing. Here the routing happens once and only the
 * matches are invoked.
 *
 * <p>Registration is the scanner's second phase (every singleton already exists), while delivery
 * only starts with the engine, which is a {@code SmartLifecycle} — so the list is complete before
 * the first event. {@code CopyOnWriteArrayList} covers what Spring's background bootstrap creates:
 * registration from several bean-creation threads, against a reader that must never see a torn
 * list.
 */
public final class OnExecutionRegistry implements ExecutionListener {

    private static final Logger log = LoggerFactory.getLogger(OnExecutionRegistry.class);

    /** Shared because a no-argument method's argument array is always the same empty one. */
    private static final Object[] NO_ARGUMENTS = new Object[0];

    private final List<Subscription> subscriptions = new CopyOnWriteArrayList<>();

    /**
     * Where an event goes: its type, and the job it belongs to — {@code null} for a batch, which
     * belongs to none.
     */
    private record Routing(ExecutionEventType type, @Nullable String job) {
    }

    private record Subscription(ExecutionEventType eventType, String job, Object bean, Method method,
            boolean takesEvent, String declaringMethod) {

        boolean matches(Routing routing) {
            if (eventType != routing.type()) {
                return false;
            }
            // An empty filter observes every job; a batch has no job, so only the empty filter sees it
            return job.isEmpty() || job.equals(routing.job());
        }

        Object[] arguments(ExecutionEvent event) {
            return takesEvent ? new Object[] {event} : NO_ARGUMENTS;
        }
    }

    void register(OnExecution annotation, Object bean, Method method, String declaringMethod) {
        Objects.requireNonNull(annotation, "annotation");
        method.setAccessible(true);
        subscriptions.add(new Subscription(annotation.event(), annotation.job(), bean, method,
                method.getParameterCount() == 1, declaringMethod));
    }

    /**
     * One failing method must not silence the others, so each invocation is isolated: the engine
     * already treats a listener's exception as that listener's problem, and inside this listener the
     * unit of failure is the annotated method rather than the whole fan-out.
     */
    @Override
    public void on(ExecutionEvent event) {
        Routing routing = routingOf(event);
        for (Subscription subscription : subscriptions) {
            if (!subscription.matches(routing)) {
                continue;
            }
            try {
                subscription.method().invoke(subscription.bean(), subscription.arguments(event));
            } catch (InvocationTargetException e) {
                log.warn("@OnExecution method {} threw — the execution is unaffected",
                        subscription.declaringMethod(), e.getCause());
            } catch (ReflectiveOperationException | RuntimeException e) {
                log.warn("@OnExecution method {} could not be invoked", subscription.declaringMethod(), e);
            }
        }
    }

    /**
     * The sealed hierarchy is what makes this safe: a new {@link ExecutionEvent} variant breaks this
     * switch at COMPILE time, so an event can never silently stop reaching its subscribers.
     */
    private static Routing routingOf(ExecutionEvent event) {
        return switch (event) {
            case Enqueued e -> new Routing(ExecutionEventType.ENQUEUED, e.jobKey().value());
            case Started e -> new Routing(ExecutionEventType.STARTED, e.jobKey().value());
            case AttemptFailed e -> new Routing(ExecutionEventType.ATTEMPT_FAILED, e.jobKey().value());
            case RetryScheduled e -> new Routing(ExecutionEventType.RETRY_SCHEDULED, e.jobKey().value());
            case Succeeded e -> new Routing(ExecutionEventType.SUCCEEDED, e.jobKey().value());
            case Failed e -> new Routing(ExecutionEventType.FAILED, e.jobKey().value());
            case Cancelled e -> new Routing(ExecutionEventType.CANCELLED, e.jobKey().value());
            case BatchCompleted _ -> new Routing(ExecutionEventType.BATCH_COMPLETED, null);
        };
    }

    /** The record a declared {@link ExecutionEventType} arrives as — the type an annotated parameter has to accept. */
    static Class<? extends ExecutionEvent> eventClass(ExecutionEventType type) {
        return switch (type) {
            case ENQUEUED -> Enqueued.class;
            case STARTED -> Started.class;
            case ATTEMPT_FAILED -> AttemptFailed.class;
            case RETRY_SCHEDULED -> RetryScheduled.class;
            case SUCCEEDED -> Succeeded.class;
            case FAILED -> Failed.class;
            case CANCELLED -> Cancelled.class;
            case BATCH_COMPLETED -> BatchCompleted.class;
        };
    }

    /**
     * Every rejection here is a subscription that would never fire, or would fire into a parameter
     * that cannot hold the event — both of them silent at runtime, which is what a boot check is
     * for.
     */
    static void validate(OnExecution annotation, Method method, String declaringMethod) {
        Class<? extends ExecutionEvent> delivered = eventClass(annotation.event());
        if (annotation.event() == ExecutionEventType.BATCH_COMPLETED && !annotation.job().isEmpty()) {
            throw new IllegalStateException("@OnExecution on " + declaringMethod + " filters BATCH_COMPLETED by job="
                    + annotation.job()
                    + " — a batch belongs to no single job, so this method would never be called; drop the job filter");
        }
        Parameter[] parameters = method.getParameters();
        if (parameters.length > 1) {
            throw new IllegalStateException("@OnExecution on " + declaringMethod + " declares " + parameters.length
                    + " parameters — it takes either none or exactly one, of type " + delivered.getSimpleName());
        }
        if (parameters.length == 1 && !parameters[0].getType().isAssignableFrom(delivered)) {
            throw new IllegalStateException("@OnExecution on " + declaringMethod + " declares event="
                    + annotation.event() + ", which is delivered as " + delivered.getName()
                    + " — a parameter of type " + parameters[0].getType().getName() + " cannot receive it");
        }
    }
}
