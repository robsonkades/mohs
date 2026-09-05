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
package io.mohs.core.resource;

import java.time.Duration;
import java.util.Objects;

/**
 * A node-local execution capability, referenced by name in the style of {@code @Async("...")} — but
 * the bean is this spec, never a {@code java.util.concurrent.Executor}: Mohs creates and owns the
 * threads, which is a requirement for cooperative cancellation, timeout by interrupt, per-runner
 * metrics and the io-to-virtual / cpu-to-platform discipline (Effective Java Item 64: refer to
 * objects by their interface or spec, not by the concrete implementation).
 *
 * <p>A single flat record rather than one sealed per mode: {@link #maxConcurrent()} is valid only
 * for {@link RunnerMode#IO}; {@link #coreSize()}, {@link #maxSize()}, {@link #queueCapacity()} and
 * {@link #keepAlive()} only for {@link RunnerMode#CPU} — a field of the wrong mode stays zero and
 * ignored. Ergonomics (preventing {@code .coreSize()} on an IO runner) is the job of the two
 * separate builders ({@link IoBuilder}/{@link CpuBuilder}), not of the stored type.
 *
 * <p>The four {@code CPU} properties mirror Spring Boot's
 * {@code org.springframework.boot.task.TaskExecutionProperties.Pool}
 * (core-size/max-size/queue-capacity/keep-alive) — the same idea as
 * {@code spring.task.execution.pool.*} — but with deliberately different defaults: Spring defaults
 * to an effectively unbounded pool and queue because it cannot know whether the work is CPU- or
 * I/O-bound; here we know it is CPU-bound, and "backpressure at every boundary, never an unbounded
 * wait" is already a project rule.
 */
public record MohsRunner(String name, RunnerMode mode, int maxConcurrent, int coreSize, int maxSize, int queueCapacity, Duration keepAlive) {

    public MohsRunner {
        Fields.requireNotBlank(name, "name");
        Objects.requireNonNull(mode, "mode");
        Objects.requireNonNull(keepAlive, "keepAlive");
        if (mode == RunnerMode.IO) {
            requireIoShape(maxConcurrent, coreSize, maxSize, queueCapacity, keepAlive);
        } else {
            requireCpuShape(maxConcurrent, coreSize, maxSize, queueCapacity, keepAlive);
        }
    }

    private static void requireIoShape(int maxConcurrent, int coreSize, int maxSize, int queueCapacity, Duration keepAlive) {
        if (maxConcurrent < 1) {
            throw new IllegalArgumentException("maxConcurrent must be at least 1");
        }
        if (coreSize != 0 || maxSize != 0 || queueCapacity != 0 || !keepAlive.isZero()) {
            throw new IllegalArgumentException("CPU-only fields (coreSize/maxSize/queueCapacity/keepAlive) must be zero for an IO runner");
        }
    }

    private static void requireCpuShape(int maxConcurrent, int coreSize, int maxSize, int queueCapacity, Duration keepAlive) {
        if (coreSize < 1) {
            throw new IllegalArgumentException("coreSize must be at least 1");
        }
        if (maxSize < coreSize) {
            throw new IllegalArgumentException("maxSize must be >= coreSize");
        }
        if (queueCapacity < 0) {
            throw new IllegalArgumentException("queueCapacity must not be negative");
        }
        if (keepAlive.isNegative()) {
            throw new IllegalArgumentException("keepAlive must not be negative");
        }
        if (maxConcurrent != 0) {
            throw new IllegalArgumentException("maxConcurrent (IO-only field) must be zero for a CPU runner");
        }
    }

    /** An IO runner; it defaults to {@code maxConcurrent = 64}. */
    public static IoBuilder io(String name) {
        return new IoBuilder(name);
    }

    /** A CPU runner; its defaults are sized from the available processors. */
    public static CpuBuilder cpu(String name) {
        return new CpuBuilder(name);
    }

    /** Builder for a {@link RunnerMode#IO} runner — only the permit count of the {@code Semaphore} that bounds concurrency. */
    public static final class IoBuilder {
        private final String name;
        private int maxConcurrent = 64;

        private IoBuilder(String name) {
            this.name = name;
        }

        public IoBuilder maxConcurrent(int max) {
            this.maxConcurrent = max;
            return this;
        }

        public MohsRunner build() {
            return new MohsRunner(name, RunnerMode.IO, maxConcurrent, 0, 0, 0, Duration.ZERO);
        }
    }

    /**
     * Builder for a {@link RunnerMode#CPU} runner — the four Spring-style pool properties.
     *
     * <p>{@code maxSize} defaults to {@code coreSize} (a fixed pool, no elasticity: more threads
     * than cores does not help CPU-bound work); {@code queueCapacity} defaults to 0 (direct
     * hand-off — it grows to {@code maxSize}, then rejects immediately, with no hidden queue); and
     * {@code keepAlive} only takes effect when {@code maxSize > coreSize} is configured explicitly
     * ({@code ThreadPoolExecutor}'s standard semantics: keep-alive does not apply to core threads
     * by default).
     */
    public static final class CpuBuilder {
        private final String name;
        private int coreSize = Runtime.getRuntime().availableProcessors();
        private int maxSize = Runtime.getRuntime().availableProcessors();
        private int queueCapacity = 0;
        private Duration keepAlive = Duration.ofSeconds(60);

        private CpuBuilder(String name) {
            this.name = name;
        }

        public CpuBuilder coreSize(int coreSize) {
            this.coreSize = coreSize;
            return this;
        }

        public CpuBuilder maxSize(int maxSize) {
            this.maxSize = maxSize;
            return this;
        }

        public CpuBuilder queueCapacity(int queueCapacity) {
            this.queueCapacity = queueCapacity;
            return this;
        }

        public CpuBuilder keepAlive(Duration keepAlive) {
            this.keepAlive = keepAlive;
            return this;
        }

        public MohsRunner build() {
            return new MohsRunner(name, RunnerMode.CPU, 0, coreSize, maxSize, queueCapacity, keepAlive);
        }
    }
}
