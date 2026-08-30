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

import java.time.Duration;
import java.util.List;
import java.util.Map;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;

import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import io.mohs.core.resource.MohsRunner;
import io.mohs.core.resource.RunnerMode;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Direct unit tests of the assembly — the validation scenarios that used to exist only through
 * {@code ApplicationContextRunner} (a full boot plus H2). The end-to-end thread is still proven in
 * {@code MohsAutoConfigurationTest#propertyDefinedRunnerIsResolvable} and
 * {@code #beanDeclaredRunnerIsCollected}.
 */
class MohsRunnersTest {

    private static MohsProperties props(Map<String, MohsProperties.Runner> runners) {
        return new MohsProperties(
                true,
                new MohsProperties.Jdbc(null, true),
                new MohsProperties.Engine(Duration.ofSeconds(5), Duration.ofSeconds(5), 50, 1, Duration.ofSeconds(30), Duration.ofSeconds(15), null, Duration.ofSeconds(60), Duration.ofDays(7), 64, 16, false),
                new MohsProperties.Lifecycle(MohsProperties.Lifecycle.StartMode.AUTO,
                        new MohsProperties.Lifecycle.Shutdown(Duration.ofSeconds(30))),
                new MohsProperties.Time(MohsProperties.Time.Mode.APPLICATION, Duration.ofSeconds(1), Duration.ofSeconds(30)),
                new MohsProperties.Registration(MohsProperties.Registration.OnConflict.OVERRIDE),
                new MohsProperties.Api(false, "/api/mohs/v1"),
                runners, Map.of());
    }

    private static MohsProperties.Runner runnerSpec(RunnerMode mode) {
        return new MohsProperties.Runner(mode, null, null, null, null, null);
    }

    @Test
    void builtInIoAndCpuAreAlwaysPresent() {
        List<MohsRunner> assembled = MohsRunners.assemble(props(Map.of()), List.of());

        assertThat(assembled).extracting(MohsRunner::name).containsExactly("io", "cpu");
        // io reuses mohs.engine.dispatch-concurrency as its maxConcurrent (the documented default)
        assertThat(assembled.getFirst().maxConcurrent()).isEqualTo(64);
        assertThat(assembled.getLast().mode()).isEqualTo(RunnerMode.CPU);
    }

    @Test
    void propertyOverridesBuiltInRunner() {
        MohsProperties.Runner io = new MohsProperties.Runner(RunnerMode.IO, 8, null, null, null, null);

        List<MohsRunner> assembled = MohsRunners.assemble(props(Map.of("io", io)), List.of());

        assertThat(assembled).filteredOn(runner -> runner.name().equals("io"))
                .singleElement()
                .extracting(MohsRunner::maxConcurrent).isEqualTo(8);
    }

    @Test
    void beanOverridesBuiltInRunner() {
        MohsRunner io = MohsRunner.io("io").maxConcurrent(4).build();

        List<MohsRunner> assembled = MohsRunners.assemble(props(Map.of()), List.of(io));

        assertThat(assembled).filteredOn(runner -> runner.name().equals("io"))
                .singleElement()
                .extracting(MohsRunner::maxConcurrent).isEqualTo(4);
    }

    /** An {@code io} runner overridden below dispatch-concurrency breaks the clamp's premise — a boot WARN naming both values, never silent degradation. */
    @Test
    void ioRunnerSmallerThanTheClaimBoundIsWarnedAtAssembly() {
        ch.qos.logback.classic.Logger logger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(MohsRunners.class);
        ListAppender<ILoggingEvent> warnWatcher = new ListAppender<>();
        warnWatcher.start();
        logger.addAppender(warnWatcher);
        try {
            MohsProperties.Runner io = new MohsProperties.Runner(RunnerMode.IO, 8, null, null, null, null);

            MohsRunners.assemble(props(Map.of("io", io)), List.of());

            assertThat(warnWatcher.list).anyMatch(event -> event.getFormattedMessage()
                    .contains("max-concurrent 8 below mohs.engine.dispatch-concurrency 64"));
        } finally {
            logger.detachAppender(warnWatcher);
        }
    }

    @Test
    void duplicateNameBetweenPropertyAndBeanFails() {
        Map<String, MohsProperties.Runner> runners = Map.of("batch", runnerSpec(RunnerMode.CPU));
        List<MohsRunner> beans = List.of(MohsRunner.cpu("batch").build());

        assertThatThrownBy(() -> MohsRunners.assemble(props(runners), beans))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("runner 'batch' declared more than once: mohs.runners.batch and @Bean MohsRunner batch");
    }

    /** core-size is a CPU field and the default mode is io: an error pointing at the property, never a silent discard (which would become a 64-thread IO runner doing CPU-bound work). */
    @Test
    void wrongModeFieldFailsNamingTheProperty() {
        MohsProperties.Runner spec = new MohsProperties.Runner(RunnerMode.IO, null, 2, null, null, null);

        assertThatThrownBy(() -> MohsRunners.assemble(props(Map.of("batch", spec)), List.of()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("invalid runner declared at mohs.runners.batch.*: core-size does not apply to mode=IO"
                        + " — change mohs.runners.batch.mode or remove mohs.runners.batch.core-size");
    }

    /** The builder's IAE ("maxSize must be >= coreSize") alone names neither the runner nor the property — and core-size's default depends on the machine; the wrapper supplies the context. */
    @Test
    void builderRejectionIsWrappedWithThePropertyPrefix() {
        MohsProperties.Runner spec = new MohsProperties.Runner(RunnerMode.CPU, null, 4, 2, null, null);

        assertThatThrownBy(() -> MohsRunners.assemble(props(Map.of("batch", spec)), List.of()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("invalid runner declared at mohs.runners.batch.*: maxSize must be >= coreSize")
                .cause().isInstanceOf(IllegalArgumentException.class);
    }
}
