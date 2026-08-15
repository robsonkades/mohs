package io.mohs.engine;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.core.task.TaskRejectedException;

import io.mohs.core.resource.MohsRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RunnerRegistryTest {

    private static MohsRunner io(String name) {
        return MohsRunner.io(name).maxConcurrent(4).build();
    }

    private static MohsRunner cpu(String name) {
        return MohsRunner.cpu(name).coreSize(1).maxSize(1).build();
    }

    @Test
    void constructorRejectsMissingDefaultRunner() {
        assertThatThrownBy(() -> new RunnerRegistry(List.of(cpu("cpu"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(RunnerRegistry.DEFAULT_RUNNER);
    }

    @Test
    void constructorRejectsDuplicateRunnerNames() {
        assertThatThrownBy(() -> new RunnerRegistry(List.of(io("io"), io("io"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("duplicate");
    }

    @Test
    void resolveNullReturnsTheDefaultRunner() throws Exception {
        try (RunnerRegistry registry = new RunnerRegistry(List.of(io("io"), cpu("cpu")))) {
            assertThat(threadNameOf(registry.resolve(null))).startsWith("mohs-runner-io-");
        }
    }

    @Test
    void resolveByNameReturnsTheNamedRunner() throws Exception {
        try (RunnerRegistry registry = new RunnerRegistry(List.of(io("io"), io("s3")))) {
            assertThat(threadNameOf(registry.resolve("s3"))).startsWith("mohs-runner-s3-");
        }
    }

    @Test
    void resolveUnknownNameThrows() throws Exception {
        try (RunnerRegistry registry = new RunnerRegistry(List.of(io("io")))) {
            assertThatThrownBy(() -> registry.resolve("ghost")).isInstanceOf(NoSuchElementException.class);
        }
    }

    /** O binder do Spring minusculiza chave de mapa não-bracketed — nome que difere só por caixa ganha diagnóstico que ensina a causa, não só "not found". */
    @Test
    void resolveNameDifferingOnlyInCaseTeachesTheCause() throws Exception {
        try (RunnerRegistry registry = new RunnerRegistry(List.of(io("io"), io("myupload")))) {
            assertThatThrownBy(() -> registry.resolve("myUpload"))
                    .isInstanceOf(NoSuchElementException.class)
                    .hasMessageContaining("'myupload'")
                    .hasMessageContaining("case-sensitive");
        }
    }

    @Test
    void ioRunnerUsesVirtualThreads() throws Exception {
        try (RunnerRegistry registry = new RunnerRegistry(List.of(io("io")))) {
            AtomicReference<Thread> threadRef = new AtomicReference<>();
            CountDownLatch done = new CountDownLatch(1);
            registry.resolve("io").execute(() -> {
                threadRef.set(Thread.currentThread());
                done.countDown();
            });
            assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(threadRef.get().isVirtual()).isTrue();
        }
    }

    @Test
    void cpuRunnerUsesPlatformThreads() throws Exception {
        try (RunnerRegistry registry = new RunnerRegistry(List.of(io("io"), cpu("cpu")))) {
            AtomicReference<Thread> threadRef = new AtomicReference<>();
            CountDownLatch done = new CountDownLatch(1);
            registry.resolve("cpu").execute(() -> {
                threadRef.set(Thread.currentThread());
                done.countDown();
            });
            assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(threadRef.get().isVirtual()).isFalse();
        }
    }

    /** close() usa o protocolo certo por tipo (close() pro IO, destroy() pro CPU) — depois de fechado, os dois rejeitam trabalho novo. */
    @Test
    void closeShutsDownBothIoAndCpuExecutors() {
        RunnerRegistry registry = new RunnerRegistry(List.of(io("io"), cpu("cpu")));
        AsyncTaskExecutor ioExecutor = registry.resolve("io");
        AsyncTaskExecutor cpuExecutor = registry.resolve("cpu");

        registry.close();

        assertThatThrownBy(() -> ioExecutor.execute(() -> { })).isInstanceOf(TaskRejectedException.class);
        assertThatThrownBy(() -> cpuExecutor.execute(() -> { })).isInstanceOf(TaskRejectedException.class);
    }

    private static String threadNameOf(AsyncTaskExecutor executor) throws InterruptedException {
        AtomicReference<Thread> threadRef = new AtomicReference<>();
        CountDownLatch done = new CountDownLatch(1);
        executor.execute(() -> {
            threadRef.set(Thread.currentThread());
            done.countDown();
        });
        assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
        return threadRef.get().getName();
    }
}
