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
 * Unitários diretos da montagem — os cenários de validação que antes só
 * existiam via {@code ApplicationContextRunner} (boot completo + H2). O
 * fio de ponta a ponta continua provado em
 * {@code MohsAutoConfigurationTest#propertyDefinedRunnerIsResolvable}/
 * {@code #beanDeclaredRunnerIsCollected}.
 */
class MohsRunnersTest {

    private static MohsProperties props(Map<String, MohsProperties.Runner> runners) {
        return new MohsProperties(
                true,
                new MohsProperties.Jdbc(null),
                new MohsProperties.Engine(Duration.ofSeconds(5), 50, 1, Duration.ofSeconds(30), null, Duration.ofSeconds(60), 64, 16, false),
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
        // io reaproveita mohs.engine.dispatch-concurrency como maxConcurrent (documento mestre §3)
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

    /** ADR-0039: io sobrescrito abaixo de dispatch-concurrency quebra a premissa do clamp — WARN de boot nomeando os dois valores, nunca degradação silenciosa. */
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

    /** core-size é campo de CPU e o mode default é io: erro apontando a propriedade, nunca descarte silencioso (que viraria runner IO de 64 threads pra trabalho CPU-bound). */
    @Test
    void wrongModeFieldFailsNamingTheProperty() {
        MohsProperties.Runner spec = new MohsProperties.Runner(RunnerMode.IO, null, 2, null, null, null);

        assertThatThrownBy(() -> MohsRunners.assemble(props(Map.of("batch", spec)), List.of()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("invalid runner declared at mohs.runners.batch.*: core-size does not apply to mode=IO"
                        + " — change mohs.runners.batch.mode or remove mohs.runners.batch.core-size");
    }

    /** A IAE do builder ("maxSize must be >= coreSize") sozinha não diz qual runner nem qual propriedade — e core-size default depende da máquina; o embrulho dá o contexto. */
    @Test
    void builderRejectionIsWrappedWithThePropertyPrefix() {
        MohsProperties.Runner spec = new MohsProperties.Runner(RunnerMode.CPU, null, 4, 2, null, null);

        assertThatThrownBy(() -> MohsRunners.assemble(props(Map.of("batch", spec)), List.of()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("invalid runner declared at mohs.runners.batch.*: maxSize must be >= coreSize")
                .cause().isInstanceOf(IllegalArgumentException.class);
    }
}
