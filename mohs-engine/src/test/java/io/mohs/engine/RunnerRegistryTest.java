package io.mohs.engine;

import java.time.Duration;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskRejectedException;

import io.mohs.core.RunnerSnapshot;
import io.mohs.core.resource.MohsRunner;
import io.mohs.core.resource.RunnerMode;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.assertj.core.api.Assertions.tuple;

class RunnerRegistryTest {

    private static MohsRunner io(String name) {
        return MohsRunner.io(name).maxConcurrent(4).build();
    }

    private static MohsRunner cpu(String name) {
        return MohsRunner.cpu(name).coreSize(1).maxSize(1).build();
    }

    /** Executor de enfeite pros testes de ciclo de vida: quem importa ali é o {@code shutdown}, nunca a contagem. */
    private static RunnerRegistry.CountingExecutor countingExecutor() {
        return new RunnerRegistry.CountingExecutor(new SimpleAsyncTaskExecutor());
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
                return new RunnerRegistry.LiveRunner(spec, countingExecutor(), () -> ioShutDown.set(true));
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
            case "io" -> new RunnerRegistry.LiveRunner(spec, countingExecutor(), () -> { throw ioFailure; });
            case "cpu" -> new RunnerRegistry.LiveRunner(spec, countingExecutor(), () -> { throw cpuFailure; });
            default -> new RunnerRegistry.LiveRunner(spec, countingExecutor(), () -> s3ShutDown.set(true));
        };
        RunnerRegistry registry = new RunnerRegistry(List.of(io("io"), cpu("cpu"), io("s3")), factory);

        // Map.copyOf não preserva ordem de inserção — qual falha vem primeiro é indeterminado
        Throwable thrown = catchThrowable(registry::close);
        assertThat(thrown).isIn(ioFailure, cpuFailure);
        assertThat(thrown.getSuppressed()).containsExactly(thrown == ioFailure ? cpuFailure : ioFailure);
        assertThat(s3ShutDown).isTrue();
    }

    /** O que o {@code GET /runners} promete: nome, modo e teto declarado, por runner, em ordem estável. */
    @Test
    void snapshotsReportTheDeclaredModeAndCeiling() {
        try (RunnerRegistry registry = new RunnerRegistry(
                List.of(MohsRunner.io("io").maxConcurrent(200).build(), MohsRunner.cpu("crunch").coreSize(2).maxSize(8).build()))) {

            assertThat(registry.snapshots())
                    .extracting(RunnerSnapshot::name, RunnerSnapshot::mode, RunnerSnapshot::max, RunnerSnapshot::running)
                    .containsExactly(
                            tuple("crunch", RunnerMode.CPU, 8, 0),
                            tuple("io", RunnerMode.IO, 200, 0));
        }
    }

    /**
     * A ocupação sobe enquanto a task roda e volta quando ela termina — a
     * task segura o contador aberto num latch, senão o teste mediria o
     * depois e passaria com o contador quebrado.
     */
    @Test
    void runningCountsWhatIsInFlightAndReleasesOnCompletion() throws InterruptedException {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try (RunnerRegistry registry = new RunnerRegistry(List.of(io("io")))) {
            registry.resolve("io").execute(() -> {
                started.countDown();
                await(release);
            });

            assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(runningOf(registry, "io")).isEqualTo(1);

            release.countDown();
            Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> runningOf(registry, "io") == 0);
        }
    }

    /** Handler que estoura tem que devolver a vaga: sem isso o contador só sobe e o número vira ficção. */
    @Test
    void runningIsReleasedWhenTheTaskThrows() throws InterruptedException {
        CountDownLatch ran = new CountDownLatch(1);
        try (RunnerRegistry registry = new RunnerRegistry(List.of(io("io")))) {
            registry.resolve("io").execute(() -> {
                ran.countDown();
                throw new IllegalStateException("handler blew up");
            });

            assertThat(ran.await(5, TimeUnit.SECONDS)).isTrue();
            Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> runningOf(registry, "io") == 0);
        }
    }

    /**
     * A parte do contrato que só existia em prosa: no modo CPU, {@code running}
     * inclui quem espera na fila. Medir só o que ocupa thread esconderia o
     * acúmulo — que é exatamente o que o operador precisa ver quando o pool
     * não vaza mas também não anda.
     */
    @Test
    void cpuRunningIncludesWhatIsWaitingInTheQueue() throws InterruptedException {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        MohsRunner singleThreaded = MohsRunner.cpu("crunch").coreSize(1).maxSize(1).queueCapacity(2).build();

        try (RunnerRegistry registry = new RunnerRegistry(List.of(io("io"), singleThreaded))) {
            AsyncTaskExecutor crunch = registry.resolve("crunch");
            crunch.execute(() -> {
                started.countDown();
                await(release);
            });
            assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
            crunch.execute(() -> await(release));
            crunch.execute(() -> await(release));

            // uma na thread, duas na fila
            assertThat(runningOf(registry, "crunch")).isEqualTo(3);

            release.countDown();
            Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> runningOf(registry, "crunch") == 0);
        }
    }

    /** Submissão recusada não ocupou nada: sem devolver a vaga, a fila cheia empurraria o contador para cima de vez. */
    @Test
    void aRejectedSubmissionDoesNotCount() throws InterruptedException {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        MohsRunner singleThreaded = MohsRunner.cpu("crunch").coreSize(1).maxSize(1).queueCapacity(1).build();

        try (RunnerRegistry registry = new RunnerRegistry(List.of(io("io"), singleThreaded))) {
            AsyncTaskExecutor crunch = registry.resolve("crunch");
            crunch.execute(() -> {
                started.countDown();
                await(release);
            });
            assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
            crunch.execute(() -> await(release));

            assertThatThrownBy(() -> crunch.execute(() -> { })).isInstanceOf(TaskRejectedException.class);

            assertThat(runningOf(registry, "crunch")).isEqualTo(2);
            release.countDown();
        }
    }

    private static int runningOf(RunnerRegistry registry, String name) {
        return registry.snapshots().stream()
                .filter(snapshot -> snapshot.name().equals(name))
                .findFirst()
                .orElseThrow()
                .running();
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) {
                throw new IllegalStateException("latch never released");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
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
