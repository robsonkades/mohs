package io.mohs.engine;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import org.junit.jupiter.api.Test;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskRejectedException;

import io.mohs.core.resource.MohsRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

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

    /** A promessa do construtor ("nenhum pool órfão") vale pra falha NO MEIO da construção, não só pra pré-validação — inatingível com os builders reais, daí a fábrica injetada. */
    @Test
    void buildFailureMidConstructionClosesTheAlreadyBuiltRunners() {
        AtomicBoolean ioShutDown = new AtomicBoolean();
        RuntimeException boom = new IllegalStateException("cpu pool failed to initialize");
        Function<MohsRunner, RunnerRegistry.LiveRunner> factory = spec -> {
            if (spec.name().equals("io")) {
                return new RunnerRegistry.LiveRunner(new SimpleAsyncTaskExecutor(), () -> ioShutDown.set(true));
            }
            throw boom;
        };

        assertThatThrownBy(() -> new RunnerRegistry(List.of(io("io"), cpu("cpu")), factory)).isSameAs(boom);
        assertThat(ioShutDown).isTrue();
    }

    /** Sem best-effort, o vizinho que falhou ao morrer deixaria vivo um pool CPU de platform threads não-daemon — que segura o shutdown da JVM inteira. */
    @Test
    void closeIsBestEffortAndRethrowsTheFirstFailureWithTheRestSuppressed() {
        AtomicBoolean s3ShutDown = new AtomicBoolean();
        RuntimeException ioFailure = new IllegalStateException("io refused to die");
        RuntimeException cpuFailure = new IllegalStateException("cpu refused to die");
        Function<MohsRunner, RunnerRegistry.LiveRunner> factory = spec -> switch (spec.name()) {
            case "io" -> new RunnerRegistry.LiveRunner(new SimpleAsyncTaskExecutor(), () -> { throw ioFailure; });
            case "cpu" -> new RunnerRegistry.LiveRunner(new SimpleAsyncTaskExecutor(), () -> { throw cpuFailure; });
            default -> new RunnerRegistry.LiveRunner(new SimpleAsyncTaskExecutor(), () -> s3ShutDown.set(true));
        };
        RunnerRegistry registry = new RunnerRegistry(List.of(io("io"), cpu("cpu"), io("s3")), factory);

        // Map.copyOf não preserva ordem de inserção — qual falha vem primeiro é indeterminado
        Throwable thrown = catchThrowable(registry::close);
        assertThat(thrown).isIn(ioFailure, cpuFailure);
        assertThat(thrown.getSuppressed()).containsExactly(thrown == ioFailure ? cpuFailure : ioFailure);
        assertThat(s3ShutDown).isTrue();
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
