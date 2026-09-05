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
package io.mohs.demo.examples;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import io.mohs.core.definition.OnDemandJob;
import io.mohs.core.definition.RecurringJob;
import io.mohs.core.resource.MohsRunner;
import io.mohs.core.resource.RunnerMode;

/**
 * <b>Scenario 11 — where a job's work actually runs.</b>
 *
 * <p>A runner is a NAMED execution capability of this node, referenced from a job the way
 * {@code @Async("...")} references an executor. The bean is the <b>spec</b>, never a
 * {@code java.util.concurrent.Executor}: Mohs creates and owns the threads, because cooperative
 * cancellation, timeout by interrupt, per-runner metrics and the IO-versus-CPU discipline are only
 * possible when the threads belong to the engine.
 *
 * <p>Two runners always exist, whether or not anything declares them: {@code io} (the default, sized
 * from {@code mohs.engine.dispatch-concurrency}) and {@code cpu}. A job that names no runner gets
 * {@code io}.
 *
 * <h2>Classify the work first — the mode is not a preference</h2>
 *
 * <table border="1">
 * <caption>{@link RunnerMode}</caption>
 * <tr><th></th><th>{@code IO}</th><th>{@code CPU}</th></tr>
 * <tr><td>For</td><td>database, HTTP, files, messaging</td><td>computation that keeps a core busy</td></tr>
 * <tr><td>Threads</td><td>virtual, one per task</td><td>a bounded platform pool</td></tr>
 * <tr><td>Bounded by</td><td>a {@code Semaphore} ({@code maxConcurrent})</td><td>{@code coreSize}/{@code maxSize}/{@code queueCapacity}</td></tr>
 * <tr><td>Sizing intuition</td><td>as many as the downstream tolerates</td><td>never more threads than cores</td></tr>
 * </table>
 *
 * <p>Getting this backwards is expensive in both directions: CPU-bound work on virtual threads
 * merely oversubscribes the carriers with no gain, and I/O-bound work on a small fixed pool blocks
 * platform threads that could have been thousands of parked virtual ones.
 *
 * <p>Backpressure is by REJECTION, never by an unbounded queue. A CPU runner defaults to
 * {@code queueCapacity = 0} — direct hand-off, then reject — because a hidden queue converts a
 * capacity problem into a latency problem you find out about much later.
 *
 * <h2>Two ways to declare one, and the rule between them</h2>
 *
 * <p>A {@code @Bean MohsRunner} (below) or a property:
 *
 * {@snippet lang="yaml" :
 * mohs:
 *   runners:
 *     example-io:
 *       mode: io
 *       max: 16
 *}
 *
 * <p>A name declared in both places is a boot error — an identity conflict always fails rather than
 * picking a winner. Overriding a built-in ({@code io}/{@code cpu}) is allowed; shrinking {@code io}
 * below {@code mohs.engine.dispatch-concurrency} logs a WARN, because the claim bound follows that
 * property and the surplus would sit RUNNING until the reaper collects it.
 *
 * <p>A field belonging to the other mode is also a boot error, never a silent discard:
 * {@code core-size} on an {@code io} runner would otherwise leave 64 virtual threads chewing
 * CPU-bound work with nothing to show for it.
 */
@Configuration(proxyBeanMethods = false)
public class RunnerExample {

    /**
     * Creates a {@code RunnerExample} instance.
     */
    public RunnerExample() {
    }

    private static final Logger log = LoggerFactory.getLogger(RunnerExample.class);

    /**
     * I/O-bound work with a deliberately small ceiling: the partner API tolerates sixteen
     * concurrent calls, so this is the semaphore that says so — in one place, rather than in every
     * handler that calls it.
     */
    @Bean
    MohsRunner exampleIoRunner() {
        return MohsRunner
                .io("example-io")
                .maxConcurrent(16)
                .build();
    }

    /**
     * CPU-bound work, kept off the I/O runner so a burst of PDF rendering cannot starve every
     * database call on the node. Two threads, no queue: what does not fit is rejected now rather
     * than queued to be late later.
     */
    @Bean
    MohsRunner exampleCpuRunner() {
        return MohsRunner
                .cpu("example-cpu")
                .coreSize(2)
                .maxSize(2)
                .queueCapacity(0)
                .build();
    }

    /** Names the runner it belongs on. An unknown name fails the boot — running elsewhere is not a safe default. */
    @OnDemandJob(id = "example-partner-call", runner = "example-io", retries = 3, timeout = "PT1M")
    void callPartner() {
        log.info("one of at most sixteen concurrent calls on this node");
    }

    @RecurringJob(id = "example-render-statements", every = "PT12H", runner = "example-cpu", timeout = "PT20M")
    void renderStatements() {
        log.info("rendering, on a platform thread that owns its core");
    }
}
