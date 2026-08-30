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

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ThreadPoolExecutor;

import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

/**
 * Mohs's central executor and scheduler factory — no engine class creates an
 * {@code Executor}/{@code ScheduledExecutorService} by hand; they all receive what this factory
 * builds, injected through the constructor (and the caller also owns the lifecycle of what it gets
 * back — this factory never shuts down what it creates; see each method's Javadoc).
 *
 * <p>It is the single point where the project's concurrency discipline becomes code rather than a
 * convention repeated in every new class: threads always named ({@code mohs-<resource>-N}), I/O-bound
 * work always on virtual threads with a real limit through a {@code Semaphore} (never through pool
 * size), CPU-bound work always on an explicitly bounded pool, and never an unbounded queue "by
 * Spring's omission" (a {@link ThreadPoolTaskExecutor} without an explicit
 * {@code setQueueCapacity} would inherit an effectively unlimited queue — exactly what is
 * forbidden).
 *
 * <p>The engine uses Spring's infrastructure freely — the three classes used
 * here already implement much of what the engine would otherwise rewrite by hand:
 * {@code ExecutorConfigurationSupport} (the parent of {@link ThreadPoolTaskExecutor} and
 * {@link ThreadPoolTaskScheduler}) implements {@code SmartLifecycle} directly — the same hook already
 * chosen for the engine's graceful shutdown.
 */
public final class MohsExecutors {

    /** The same default as {@code mohs.lifecycle.shutdown.grace-period} — not an arbitrary number chosen separately. */
    private static final int AWAIT_TERMINATION_SECONDS = 30;

    private MohsExecutors() {
    }

    /**
     * I/O-bound: one virtual thread per task, with a real concurrency ceiling through Spring's
     * internal {@code Semaphore} — never through pool size. Above the limit, an explicit
     * {@link java.util.concurrent.RejectedExecutionException}: backpressure, not a hidden queue nor
     * an unbounded wait.
     *
     * <p>The lifecycle belongs to the caller. It returns the concrete type (rather than the
     * {@code AsyncTaskExecutor} interface, as this method used to) because
     * {@link SimpleAsyncTaskExecutor} is {@code AutoCloseable} ({@code close()}) and has no
     * {@code shutdown()} — whoever received the executor needs the concrete type to be able to close
     * it, the same reason {@link #cpuBoundExecutor} and {@link #scheduler} return their concrete
     * types.
     */
    public static SimpleAsyncTaskExecutor ioBoundExecutor(String namePrefix, int concurrencyLimit) {
        requireNotBlank(namePrefix, "namePrefix");
        if (concurrencyLimit <= 0) {
            throw new IllegalArgumentException("concurrencyLimit must be positive");
        }

        SimpleAsyncTaskExecutor executor = new SimpleAsyncTaskExecutor(threadNamePrefix(namePrefix));
        executor.setVirtualThreads(true);
        executor.setConcurrencyLimit(concurrencyLimit);
        executor.setRejectTasksWhenLimitReached(true);
        executor.setTaskTerminationTimeout(Duration.ofSeconds(AWAIT_TERMINATION_SECONDS).toMillis());
        return executor;
    }

    /**
     * CPU-bound: a bounded pool of platform threads — the same four fields and the same validation as
     * {@link io.mohs.core.resource.MohsRunner#cpu} (not a coincidence: the spec was designed in that
     * shape).
     *
     * <p>It calls {@code initialize()} before returning — with no {@code ApplicationContext} managing
     * the bean, nobody else would ({@code ExecutorConfigurationSupport}'s initialisation normally
     * comes from {@code InitializingBean#afterPropertiesSet}). From here on the lifecycle belongs to
     * the caller: this factory does not shut down what it creates.
     */
    public static ThreadPoolTaskExecutor cpuBoundExecutor(String namePrefix, int coreSize, int maxSize, int queueCapacity, Duration keepAlive) {
        requireNotBlank(namePrefix, "namePrefix");
        if (coreSize < 1) {
            throw new IllegalArgumentException("coreSize must be at least 1");
        }
        if (maxSize < coreSize) {
            throw new IllegalArgumentException("maxSize must be >= coreSize");
        }
        if (queueCapacity < 0) {
            throw new IllegalArgumentException("queueCapacity must not be negative");
        }
        Objects.requireNonNull(keepAlive, "keepAlive");
        if (keepAlive.isNegative()) {
            throw new IllegalArgumentException("keepAlive must not be negative");
        }

        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setThreadNamePrefix(threadNamePrefix(namePrefix));
        executor.setCorePoolSize(coreSize);
        executor.setMaxPoolSize(maxSize);
        executor.setQueueCapacity(queueCapacity);
        executor.setKeepAliveSeconds((int) keepAlive.toSeconds());
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.AbortPolicy());
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(AWAIT_TERMINATION_SECONDS);
        executor.initialize();
        return executor;
    }

    /**
     * A dedicated scheduler — today the poll loop's tick ({@link Engine}, whose body does claim and
     * reaper work over blocking JDBC directly on the scheduler's thread) and the
     * {@code DatabaseClock}'s resync ({@code io.mohs.autoconfigure}), generic enough for any future
     * cycle. Both are I/O-bound by the project's classification — hence virtual threads rather than
     * platform ones.
     *
     * <p>Virtual threads here do not give up the concurrency ceiling: unlike {@link #ioBoundExecutor}
     * (thread-per-task with no limit, hence the {@code Semaphore}), {@link ThreadPoolTaskScheduler}
     * remains wrapped in a real {@code ScheduledThreadPoolExecutor(poolSize, threadFactory)} —
     * {@code setVirtualThreads(true)} only swaps the worker's thread type, and {@code poolSize}
     * remains the real ceiling on concurrent executions (confirmed in Spring 7.0.8's source, not only
     * in the Javadoc).
     */
    public static ThreadPoolTaskScheduler scheduler(String namePrefix, int poolSize) {
        requireNotBlank(namePrefix, "namePrefix");
        if (poolSize < 1) {
            throw new IllegalArgumentException("poolSize must be at least 1");
        }

        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setThreadNamePrefix(threadNamePrefix(namePrefix));
        scheduler.setVirtualThreads(true);
        scheduler.setPoolSize(poolSize);
        scheduler.setRejectedExecutionHandler(new ThreadPoolExecutor.AbortPolicy());
        scheduler.setWaitForTasksToCompleteOnShutdown(true);
        scheduler.setAwaitTerminationSeconds(AWAIT_TERMINATION_SECONDS);
        scheduler.initialize();
        return scheduler;
    }

    private static String threadNamePrefix(String namePrefix) {
        return namePrefix.endsWith("-") ? namePrefix : namePrefix + "-";
    }

    /** The same check as {@code io.mohs.core.resource.Fields#requireNotBlank}, deliberately not reused from there: a different package, and not worth exposing as public API for this (the same reasoning already recorded on that class). */
    private static void requireNotBlank(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
    }
}
