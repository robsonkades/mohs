package io.mohs.autoconfigure;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

import io.mohs.core.EngineState;
import io.mohs.core.Mohs;
import io.mohs.core.definition.JobDefinition;
import io.mohs.core.event.ExecutionListener;
import io.mohs.core.event.Succeeded;
import io.mohs.core.job.JobKey;
import io.mohs.engine.HandlerRegistry;

import static org.assertj.core.api.Assertions.assertThat;

class MohsAutoConfigurationTest {

    record Handler() {
    }

    record Greeting(String name) {
    }

    private static DataSource freshH2DataSource() {
        JdbcDataSource h2 = new JdbcDataSource();
        h2.setURL("jdbc:h2:mem:mohs-autoconfig-test-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        h2.setUser("sa");
        h2.setPassword("");
        // aplicado ANTES de registrar o bean no contexto — SmartLifecycle.start() do
        // Engine dispara no fim do refresh(), então o schema precisa existir antes disso.
        new ResourceDatabasePopulator(new ClassPathResource("schema-h2.sql")).execute(h2);
        return h2;
    }

    private static ApplicationContextRunner runnerWith(DataSource dataSource, String... extraProperties) {
        String[] properties = new String[extraProperties.length + 2];
        properties[0] = "mohs.jdbc.dialect=h2";
        properties[1] = "mohs.engine.poll-interval=50ms";
        System.arraycopy(extraProperties, 0, properties, 2, extraProperties.length);
        return new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(MohsAutoConfiguration.class))
                .withBean(DataSource.class, () -> dataSource)
                .withPropertyValues(properties);
    }

    @Test
    void contextBootsWithOnlyDialectConfigured() {
        runnerWith(freshH2DataSource()).run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).hasSingleBean(Mohs.class);
        });
    }

    @Test
    void scheduledExecutionRunsEndToEndThroughTheRealEngine() {
        CountDownLatch succeeded = new CountDownLatch(1);
        ExecutionListener listener = event -> {
            if (event instanceof Succeeded) {
                succeeded.countDown();
            }
        };

        runnerWith(freshH2DataSource())
                .withBean(ExecutionListener.class, () -> listener)
                .run(context -> {
                    context.getBean(HandlerRegistry.class).register(JobKey.of("greet"), (payload, ctx) -> { });

                    Mohs mohs = context.getBean(Mohs.class);
                    mohs.define(JobDefinition.of("greet", Handler.class, spec -> spec.onDemand().runner("io")));
                    mohs.schedule("greet", new Greeting("ana")).now();

                    // esperar DENTRO do run(): a context runner fecha o contexto (e o
                    // Engine, via SmartLifecycle.stop) assim que o lambda retorna — o
                    // claim/dispatch acontece em background, num tick futuro do poll loop.
                    assertThat(succeeded.await(5, TimeUnit.SECONDS)).as("execution claimed, dispatched and succeeded within timeout").isTrue();
                });
    }

    @Test
    void lifecycleMethodsWorkThroughTheMohsBean() {
        runnerWith(freshH2DataSource(), "mohs.lifecycle.shutdown.grace-period=2s").run(context -> {
            Mohs mohs = context.getBean(Mohs.class);

            mohs.lifecycle().pause();
            assertThat(mohs.lifecycle().state()).isEqualTo(EngineState.PAUSED);

            mohs.lifecycle().resume();
            assertThat(mohs.lifecycle().state()).isEqualTo(EngineState.RUNNING);

            mohs.lifecycle().stop(Duration.ofSeconds(2));
            assertThat(mohs.lifecycle().state()).isEqualTo(EngineState.STOPPED);
        });
    }

    @Test
    void disablingMohsRegistersNoBeans() {
        runnerWith(freshH2DataSource(), "mohs.enabled=false").run(context ->
                assertThat(context).doesNotHaveBean(Mohs.class));
    }

    private static void awaitState(io.mohs.engine.ExecutionStore executionStore, String jobKey, EngineState unused) throws InterruptedException {
        // implementado abaixo, via polling curto — ver ajuste final do teste.
    }
}
