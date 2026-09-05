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

import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import io.mohs.core.RunnerSnapshot;
import io.mohs.core.resource.MohsRunner;

/**
 * Converts a {@link MohsRunner} (a pure spec, with no {@code Executor} at all — see its class
 * Javadoc) into live executors through {@link MohsExecutors}, and owns their lifecycle: it builds
 * them here, so it closes them here ({@link #close()}) — the same ownership discipline already
 * documented in {@link MohsExecutors} ("whoever builds owns the lifecycle").
 *
 * <p>It receives the list already resolved — defaults, property overrides and conflicts between
 * configuration sources are decisions for whoever assembles the list (today,
 * {@code io.mohs.autoconfigure}), not for this class.
 */
public final class RunnerRegistry implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(RunnerRegistry.class);

    /** {@code JobDefinition.runner() == null} resolves to this name — the only runner this class insists must exist. */
    public static final String DEFAULT_RUNNER = "io";

    private final Map<String, LiveRunner> executors;

    /**
     * A live executor paired with the shutdown action born alongside it in {@link #build} —
     * {@link #close()} merely runs {@code shutdown.run()}, without re-deriving the concrete type
     * through {@code instanceof}: a new runner mode changes exactly one place.
     *
     * <p>The component is the {@link CountingExecutor} rather than the {@link AsyncTaskExecutor}
     * interface — the exception to referring to objects by their interface (Effective Java, Item 64)
     * is precisely the decorator's extra capability: the occupancy {@link #snapshot()} reads.
     */
    record LiveRunner(MohsRunner spec, CountingExecutor executor, Runnable shutdown) {

        RunnerSnapshot snapshot() {
            return new RunnerSnapshot(spec.name(), spec.mode(), maxOf(spec), executor.running());
        }
    }

    /**
     * Creates a {@code RunnerRegistry} with the supplied values.
     *
     * @param runners the declared runner configurations
     */
    public RunnerRegistry(List<MohsRunner> runners) {
        this(runners, RunnerRegistry::build);
    }

    /**
     * A test seam: the failure paths midway through construction are unreachable with the real
     * builders (the spec arrives already valid) — the injectable factory exists only to prove them.
     * Validating the specs up front covers duplicates and a missing default; the "no orphan pool"
     * guarantee for the rest is the try/catch around the construction loop.
     */
    RunnerRegistry(List<MohsRunner> runners, Function<MohsRunner, LiveRunner> factory) {
        Objects.requireNonNull(runners, "runners");
        Map<String, MohsRunner> specs = new LinkedHashMap<>();
        for (MohsRunner runner : runners) {
            if (specs.putIfAbsent(runner.name(), runner) != null) {
                throw new IllegalArgumentException("duplicate runner name '" + runner.name() + "'");
            }
        }
        if (!specs.containsKey(DEFAULT_RUNNER)) {
            throw new IllegalArgumentException(
                    "RunnerRegistry requires a '" + DEFAULT_RUNNER + "' runner (the default) — none provided: " + specs.keySet());
        }
        Map<String, LiveRunner> built = new LinkedHashMap<>();
        try {
            specs.forEach((name, spec) -> built.put(name, factory.apply(spec)));
        } catch (RuntimeException buildFailure) {
            // No orphan pool: close what has already been created and rethrow the original cause intact
            for (LiveRunner alreadyBuilt : built.values()) {
                try {
                    alreadyBuilt.shutdown().run();
                } catch (RuntimeException shutdownFailure) {
                    buildFailure.addSuppressed(shutdownFailure);
                }
            }
            throw buildFailure;
        }
        this.executors = Map.copyOf(built);
    }

    /**
     * The right shutdown protocol per concrete type — {@link SimpleAsyncTaskExecutor#close()} (IO)
     * versus {@link ThreadPoolTaskExecutor#destroy()} (CPU), the same asymmetry already documented in
     * {@link MohsExecutors} — decided here, in the one place that knows the type it built.
     */
    private static LiveRunner build(MohsRunner runner) {
        String namePrefix = "mohs-runner-" + runner.name();
        return switch (runner.mode()) {
            case IO -> {
                SimpleAsyncTaskExecutor io = MohsExecutors.ioBoundExecutor(namePrefix, runner.maxConcurrent());
                yield new LiveRunner(runner, new CountingExecutor(io), io::close);
            }
            case CPU -> {
                ThreadPoolTaskExecutor cpu = MohsExecutors.cpuBoundExecutor(namePrefix, runner.coreSize(), runner.maxSize(), runner.queueCapacity(), runner.keepAlive());
                yield new LiveRunner(runner, new CountingExecutor(cpu), cpu::destroy);
            }
        };
    }

    /**
     * A Decorator (GoF) counting occupancy where it happens, rather than asking the pool for it:
     * {@code SimpleAsyncTaskExecutor} (IO mode) does not expose its active count, and deriving it from
     * {@code ThreadPoolTaskExecutor#getActiveCount} only in CPU mode would give the same field two
     * meanings.
     *
     * <p>It increments on ACCEPTANCE and decrements on completion — including when the task throws,
     * otherwise a handler that blows up would leak the counter upwards until the number became
     * fiction. A rejection by the executor (a full queue in CPU mode) also gives the slot back, in the
     * {@code catch}: the {@code execute} did not happen.
     *
     * <p>A named class owning its own counter, rather than a lambda over an external
     * {@code AtomicInteger}: the counter and the executor feeding it only mean anything together —
     * loose in two fields, nothing stops a pair that does not talk to each other, and the number would
     * lie in silence.
     *
     * <p>It wraps once, at construction, not on every {@link #resolve}: callers keep receiving the same
     * executor as always and need not know counting exists.
     */
    static final class CountingExecutor implements AsyncTaskExecutor {

        private final AsyncTaskExecutor delegate;
        private final AtomicInteger running = new AtomicInteger();

        CountingExecutor(AsyncTaskExecutor delegate) {
            this.delegate = Objects.requireNonNull(delegate, "delegate");
        }

        @Override
        public void execute(Runnable task) {
            running.incrementAndGet();
            try {
                delegate.execute(() -> {
                    try {
                        task.run();
                    } finally {
                        running.decrementAndGet();
                    }
                });
            } catch (RuntimeException notAccepted) {
                // Gives the slot back only because the execute did NOT happen. This depends on the
                // delegate executor never running the task on the calling thread: today that holds
                // (AbortPolicy on CPU, rejectTasksWhenLimitReached on IO — see MohsExecutors), and
                // with a CallerRunsPolicy a task that throws would decrement twice, here and in the
                // finally above.
                running.decrementAndGet();
                throw notAccepted;
            }
        }

        /**
         * Never negative. A counter leaking downwards is only reachable through a defect here (a
         * double decrement), and {@code RunnerSnapshot} refuses {@code running < 0} — which would turn
         * the bug into a 500 from {@code GET /runners}, taking the healthy runners down with it and
         * filling the log on every dashboard refresh. An instrument degrades and shouts; it never
         * becomes the cause of the unavailability it was meant to explain.
         */
        int running() {
            int current = running.get();
            if (current < 0) {
                log.warn("runner occupancy counter went negative ({}) — reporting 0; this is a bug in CountingExecutor", current);
                return 0;
            }
            return current;
        }
    }

    /**
     * What this node declared, and how much each runner is carrying now.
     *
     * <p>Ordered by name because {@code executors} is a {@code Map.copyOf} — with no defined order. A
     * list that changes order between two reads would make the dashboard's table dance on every
     * refresh; alphabetical is stable and needs no explanation.
     *
     * @return the local runner configurations and current occupancy
     */
    public List<RunnerSnapshot> snapshots() {
        return executors.values().stream()
                .map(LiveRunner::snapshot)
                .sorted(Comparator.comparing(RunnerSnapshot::name))
                .toList();
    }

    /** The declared ceiling lives in a different field per mode — {@code maxConcurrent} on IO, {@code maxSize} on CPU (see {@code RunnerSnapshot}). */
    private static int maxOf(MohsRunner spec) {
        return switch (spec.mode()) {
            case IO -> spec.maxConcurrent();
            case CPU -> spec.maxSize();
        };
    }

    /**
     * {@code null} resolves to the {@link #DEFAULT_RUNNER} — the same contract already documented on
     * {@code JobDefinition.runner()}: "null uses the default runner". An unknown name throws, and the
     * caller (today, {@link Engine#submitDispatch}) decides what to do (fail only the execution, not
     * the whole node).
     *
     * @param runnerName the runner name, or {@code null} for the default runner
     * @return the executor for the selected runner
     */
    public AsyncTaskExecutor resolve(@Nullable String runnerName) {
        String name = runnerName == null ? DEFAULT_RUNNER : runnerName;
        LiveRunner runner = executors.get(name);
        if (runner == null) {
            throw new NoSuchElementException(noSuchRunnerMessage(name));
        }
        return runner.executor();
    }

    /**
     * A divergence of case alone gets its own diagnostic: Spring's binder canonicalises a
     * non-bracketed map key to lower case, so {@code mohs.runners.myUpload.*} registers the runner as
     * {@code myupload} while {@code JobDefinition.runner()} is case-sensitive — without the hint,
     * "no runner named 'myUpload'" teaches nothing about what to fix.
     */
    private String noSuchRunnerMessage(String name) {
        for (String registered : executors.keySet()) {
            if (registered.equalsIgnoreCase(name)) {
                return "no runner named '" + name + "' registered, but '" + registered + "' is — runner names are case-sensitive, and Spring's"
                        + " relaxed binding lowercases unbracketed map keys (declare it as mohs.runners.[" + name + "] to keep the exact case,"
                        + " or use the lowercase name in JobDefinition.runner())";
            }
        }
        return "no runner named '" + name + "' registered — available: " + executors.keySet();
    }

    /**
     * Best-effort: no runner stays alive because a neighbour failed to die — the CPU pool uses
     * non-daemon platform threads, which would hold up the JVM's shutdown. The first exception is
     * rethrown at the end with the others as suppressed.
     */
    @Override
    public void close() {
        RuntimeException failure = null;
        for (LiveRunner runner : executors.values()) {
            try {
                runner.shutdown().run();
            } catch (RuntimeException e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }
}
