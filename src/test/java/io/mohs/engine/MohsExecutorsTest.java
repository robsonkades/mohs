package io.mohs.engine;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
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

    /** CLAUDE.md: "Limite de concorrência com Semaphore, nunca via tamanho de pool" — aqui é o Semaphore interno do SimpleAsyncTaskExecutor. */
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

    @Test
    void schedulerRunsScheduledTaskOnNamedThread() throws Exception {
        ThreadPoolTaskScheduler scheduler = MohsExecutors.scheduler("mohs-sched-probe", 1);
        AtomicReference<Thread> threadRef = new AtomicReference<>();
        CountDownLatch fired = new CountDownLatch(1);

        scheduler.scheduleWithFixedDelay(() -> {
            threadRef.set(Thread.currentThread());
            fired.countDown();
        }, Duration.ofMillis(10));

        assertThat(fired.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(threadRef.get().getName()).startsWith("mohs-sched-probe-");
    }
}
