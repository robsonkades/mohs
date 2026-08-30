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
package io.mohs.autoconfigure;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mohs.core.resource.MohsRunner;
import io.mohs.core.resource.RunnerMode;
import io.mohs.engine.RunnerRegistry;

/**
 * Assembly of the named runners — pure wiring vocabulary: no state, no Spring, testable in
 * isolation (the same pattern as {@link MohsJobs}). {@link MohsAutoConfiguration} is its only
 * caller; package-private (Effective Java Item 15: minimise accessibility).
 */
final class MohsRunners {

    private static final Logger log = LoggerFactory.getLogger(MohsRunners.class);

    private static final String BUILT_IN = "built-in";

    private MohsRunners() {
    }

    /** A runner alongside the source that declared it — one map carries both, instead of two parallel maps sharing a key. */
    private record SourcedRunner(MohsRunner runner, String source) {
    }

    /**
     * The built-in {@code io}/{@code cpu} runners are always present (the documented document's
     * defaults — {@code io} reuses {@code mohs.engine.dispatch-concurrency}, the same role it had
     * when it was still the only fixed dispatch executor).
     *
     * <p>A built-in may be overridden by a property or a {@code @Bean}; a name duplicated between
     * {@code mohs.runners.*} and a {@code @Bean MohsRunner} is a boot error — the same "an identity
     * conflict always fails" philosophy already used for {@code annotation x programmatic} in
     * {@link MohsJobScanner}.
     */
    static List<MohsRunner> assemble(MohsProperties properties, List<MohsRunner> beanRunners) {
        Map<String, SourcedRunner> byName = new LinkedHashMap<>();
        byName.put(RunnerRegistry.DEFAULT_RUNNER, new SourcedRunner(MohsRunner.io(RunnerRegistry.DEFAULT_RUNNER).maxConcurrent(properties.engine().dispatchConcurrency()).build(), BUILT_IN));
        byName.put("cpu", new SourcedRunner(MohsRunner.cpu("cpu").build(), BUILT_IN));

        properties
                .runners()
                .forEach((name, spec) -> declare(byName, name, toMohsRunner(name, spec), "mohs.runners." + name));

        for (MohsRunner beanRunner : beanRunners) {
            declare(byName, beanRunner.name(), beanRunner, "@Bean MohsRunner " + beanRunner.name());
        }
        warnWhenIoRunnerIsSmallerThanTheClaimBound(byName, properties.engine().dispatchConcurrency());
        return byName.values().stream().map(SourcedRunner::runner).toList();
    }

    /**
     * The claim clamp uses {@code mohs.engine.dispatch-concurrency} as the node's ceiling — a
     * single-source assumption that overriding the {@code io} runner with a smaller {@code max}
     * breaks silently: the surplus between the two values goes back to being rejected by the
     * executor and stays RUNNING until the reaper picks it up, which is the pathology the clamp
     * eliminated. A WARN rather than a boot error: capping {@code io} is a legitimate operational
     * choice and the recovery path exists — the warning hands the consequence back to the operator.
     */
    private static void warnWhenIoRunnerIsSmallerThanTheClaimBound(Map<String, SourcedRunner> byName, int dispatchConcurrency) {
        MohsRunner ioRunner = byName.get(RunnerRegistry.DEFAULT_RUNNER).runner();
        if (ioRunner.mode() == RunnerMode.IO && ioRunner.maxConcurrent() < dispatchConcurrency) {
            log.warn("runner 'io' overridden with max-concurrent {} below mohs.engine.dispatch-concurrency {} — "
                    + "the claim bound follows dispatch-concurrency, so the excess will be rejected by the "
                    + "executor and sit RUNNING until the reaper reclaims it; align the two values",
                    ioRunner.maxConcurrent(), dispatchConcurrency);
        }
    }

    private static void declare(Map<String, SourcedRunner> byName, String name, MohsRunner runner, String source) {
        SourcedRunner existing = byName.get(name);
        if (existing != null && !existing.source().equals(BUILT_IN)) {
            throw new IllegalStateException("runner '" + name + "' declared more than once: " + existing.source() + " and " + source);
        }
        byName.put(name, new SourcedRunner(runner, source));
    }

    /**
     * A field belonging to the wrong mode is a boot error, never a silent discard — the same stance
     * as {@link MohsRunner}'s compact constructor, which throws for a wrong-mode field, and the
     * same "an identity conflict always fails" philosophy as {@link #declare}.
     *
     * <p>{@code core-size=2} with {@code mode} left at the {@code io} default would otherwise become
     * a runner of 64 virtual threads for CPU-bound work, with no warning at all. The builder's own
     * validation gains the context only the property has: "maxSize must be >= coreSize" alone says
     * neither which runner nor which property — and {@code core-size}'s default depends on the
     * machine's cores, so the boot would fail only in production.
     */
    private static MohsRunner toMohsRunner(String name, MohsProperties.Runner spec) {
        String prefix = "mohs.runners." + name;
        try {
            return switch (spec.mode()) {
                case IO -> {
                    requireUnset(prefix, spec.mode(), "core-size", spec.coreSize());
                    requireUnset(prefix, spec.mode(), "max-size", spec.maxSize());
                    requireUnset(prefix, spec.mode(), "queue-capacity", spec.queueCapacity());
                    requireUnset(prefix, spec.mode(), "keep-alive", spec.keepAlive());
                    MohsRunner.IoBuilder builder = MohsRunner.io(name);
                    if (spec.max() != null) {
                        builder.maxConcurrent(spec.max());
                    }
                    yield builder.build();
                }
                case CPU -> {
                    requireUnset(prefix, spec.mode(), "max", spec.max());
                    MohsRunner.CpuBuilder builder = MohsRunner.cpu(name);
                    if (spec.coreSize() != null) {
                        builder.coreSize(spec.coreSize());
                    }
                    if (spec.maxSize() != null) {
                        builder.maxSize(spec.maxSize());
                    }
                    if (spec.queueCapacity() != null) {
                        builder.queueCapacity(spec.queueCapacity());
                    }
                    if (spec.keepAlive() != null) {
                        builder.keepAlive(spec.keepAlive());
                    }
                    yield builder.build();
                }
            };
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException("invalid runner declared at " + prefix + ".*: " + e.getMessage(), e);
        }
    }

    private static void requireUnset(String prefix, RunnerMode mode, String property, @Nullable Object value) {
        if (value != null) {
            throw new IllegalStateException("invalid runner declared at " + prefix + ".*: " + property
                    + " does not apply to mode=" + mode + " — change " + prefix + ".mode or remove " + prefix + "." + property);
        }
    }
}
