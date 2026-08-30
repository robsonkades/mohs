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

    /** A placeholder executor for the lifecycle tests: what matters there is the {@code shutdown}, never the counting. */
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

    /** Spring's binder lower-cases a non-bracketed map key — a name differing only in case gets a diagnostic that teaches the cause, not just "not found". */
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

    /** close() uses the right protocol per type (close() for IO, destroy() for CPU) — once closed, both reject new work. */
    @Test
    void closeShutsDownBothIoAndCpuExecutors() {
        RunnerRegistry registry = new RunnerRegistry(List.of(io("io"), cpu("cpu")));
        AsyncTaskExecutor ioExecutor = registry.resolve("io");
        AsyncTaskExecutor cpuExecutor = registry.resolve("cpu");

        registry.close();

        assertThatThrownBy(() -> ioExecutor.execute(() -> { })).isInstanceOf(TaskRejectedException.class);
        assertThatThrownBy(() -> cpuExecutor.execute(() -> { })).isInstanceOf(TaskRejectedException.class);
    }

    /** The constructor's promise ("no orphan pool") holds for a failure MIDWAY through construction, not only for pre-validation — unreachable with the real builders, hence the injected factory. */
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

    /** Without best-effort, a neighbour that failed to die would leave a CPU pool of non-daemon platform threads alive — which holds up the whole JVM's shutdown. */
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

        // Map.copyOf does not preserve insertion order — which failure comes first is undetermined
        Throwable thrown = catchThrowable(registry::close);
        assertThat(thrown).isIn(ioFailure, cpuFailure);
        assertThat(thrown.getSuppressed()).containsExactly(thrown == ioFailure ? cpuFailure : ioFailure);
        assertThat(s3ShutDown).isTrue();
    }

    /** What {@code GET /runners} promises: name, mode and declared ceiling, per runner, in a stable order. */
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
     * Occupancy rises while the task runs and comes back when it finishes — the task holds the counter
     * open on a latch, otherwise the test would measure the aftermath and pass with a broken counter.
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

    /** A handler that blows up has to give the slot back: without that the counter only rises and the number becomes fiction. */
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
     * The part of the contract that existed only in prose: in CPU mode, {@code running} includes what
     * waits in the queue. Measuring only what occupies a thread would hide the backlog — which is
     * exactly what the operator needs to see when the pool neither leaks nor moves.
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

            // One on a thread, two in the queue
            assertThat(runningOf(registry, "crunch")).isEqualTo(3);

            release.countDown();
            Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> runningOf(registry, "crunch") == 0);
        }
    }

    /** A refused submission occupied nothing: without giving the slot back, a full queue would push the counter up for good. */
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
