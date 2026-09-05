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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.core.task.TaskRejectedException;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MohsExecutorsTest {

    @Test
    void ioBoundExecutorRejectsBlankNamePrefix() {
        assertThatThrownBy(() -> MohsExecutors.ioBoundExecutor(" ", 1)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void ioBoundExecutorRejectsNonPositiveConcurrencyLimit() {
        assertThatThrownBy(() -> MohsExecutors.ioBoundExecutor("test", 0)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void ioBoundExecutorRunsTasksOnNamedVirtualThreads() throws Exception {
        AsyncTaskExecutor executor = MohsExecutors.ioBoundExecutor("mohs-probe", 4);
        AtomicReference<Thread> threadRef = new AtomicReference<>();
        CountDownLatch done = new CountDownLatch(1);

        executor.execute(() -> {
            threadRef.set(Thread.currentThread());
            done.countDown();
        });

        assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(threadRef.get().isVirtual()).isTrue();
        assertThat(threadRef.get().getName()).startsWith("mohs-probe-");
    }

    /** "Bound concurrency with a Semaphore, never through pool size" — here it is SimpleAsyncTaskExecutor's internal Semaphore. */
    @Test
    void ioBoundExecutorRejectsTasksBeyondConcurrencyLimit() throws Exception {
        AsyncTaskExecutor executor = MohsExecutors.ioBoundExecutor("mohs-probe", 1);
        CountDownLatch firstTaskStarted = new CountDownLatch(1);
        CountDownLatch releaseFirstTask = new CountDownLatch(1);
        executor.execute(() -> {
            firstTaskStarted.countDown();
            try {
                releaseFirstTask.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        assertThat(firstTaskStarted.await(5, TimeUnit.SECONDS)).isTrue();

        assertThatThrownBy(() -> executor.execute(() -> {
        })).isInstanceOf(TaskRejectedException.class);

        releaseFirstTask.countDown();
    }

    @Test
    void cpuBoundExecutorRejectsInvalidSizes() {
        assertThatThrownBy(() -> MohsExecutors.cpuBoundExecutor("test", 0, 1, 0, Duration.ZERO)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> MohsExecutors.cpuBoundExecutor("test", 2, 1, 0, Duration.ZERO)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> MohsExecutors.cpuBoundExecutor("test", 1, 1, -1, Duration.ZERO)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> MohsExecutors.cpuBoundExecutor("test", 1, 1, 0, Duration.ofSeconds(-1))).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void cpuBoundExecutorRunsTaskOnNamedPlatformThread() throws Exception {
        ThreadPoolTaskExecutor executor = MohsExecutors.cpuBoundExecutor("mohs-cpu-probe", 1, 1, 0, Duration.ofSeconds(10));
        AtomicReference<Thread> threadRef = new AtomicReference<>();
        CountDownLatch done = new CountDownLatch(1);

        executor.execute(() -> {
            threadRef.set(Thread.currentThread());
            done.countDown();
        });

        assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(threadRef.get().isVirtual()).isFalse();
        assertThat(threadRef.get().getName()).startsWith("mohs-cpu-probe-");
    }

    @Test
    void schedulerRejectsInvalidPoolSize() {
        assertThatThrownBy(() -> MohsExecutors.scheduler("test", 0)).isInstanceOf(IllegalArgumentException.class);
    }

    /** The task's body (claim and reaper in Engine) is blocking JDBC — I/O-bound, hence virtual threads. */
    @Test
    void schedulerRunsScheduledTaskOnNamedVirtualThread() throws Exception {
        ThreadPoolTaskScheduler scheduler = MohsExecutors.scheduler("mohs-sched-probe", 1);
        AtomicReference<Thread> threadRef = new AtomicReference<>();
        CountDownLatch fired = new CountDownLatch(1);

        scheduler.scheduleWithFixedDelay(() -> {
            threadRef.set(Thread.currentThread());
            fired.countDown();
        }, Duration.ofMillis(10));

        assertThat(fired.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(threadRef.get().isVirtual()).isTrue();
        assertThat(threadRef.get().getName()).startsWith("mohs-sched-probe-");
    }

    /**
     * poolSize remains the real ceiling on concurrent executions with virtual threads —
     * {@link ThreadPoolTaskScheduler}, not thread-per-task (see {@link MohsExecutors#scheduler}'s
     * Javadoc).
     *
     * <p>Independent tasks, not a single recurring one through {@code scheduleWithFixedDelay}: that one
     * never overlaps itself by the JDK's own contract (the next execution is only scheduled once the
     * previous finishes) — it would prove nothing about {@code poolSize} and would hold for any pool
     * size. Here, {@code taskCount} distinct tasks contend for the same pool of size 1 on purpose.
     */
    @Test
    void schedulerWithPoolSizeOneNeverRunsTasksConcurrently() throws Exception {
        ThreadPoolTaskScheduler scheduler = MohsExecutors.scheduler("mohs-sched-concurrency-probe", 1);
        int taskCount = 5;
        AtomicInteger concurrentRunners = new AtomicInteger(0);
        AtomicInteger maxObservedConcurrency = new AtomicInteger(0);
        CountDownLatch allTasksRan = new CountDownLatch(taskCount);

        Runnable task = () -> {
            int current = concurrentRunners.incrementAndGet();
            maxObservedConcurrency.updateAndGet(max -> Math.max(max, current));
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                concurrentRunners.decrementAndGet();
                allTasksRan.countDown();
            }
        };
        for (int i = 0; i < taskCount; i++) {
            scheduler.execute(task);
        }

        assertThat(allTasksRan.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(maxObservedConcurrency.get()).isEqualTo(1);
    }
}
