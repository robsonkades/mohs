package io.mohs.autoconfigure;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.sql.DataSource;

import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

import io.mohs.core.EngineState;
import io.mohs.core.Mohs;
import io.mohs.core.definition.JobDefinition;
import io.mohs.core.definition.MohsJob;
import io.mohs.core.event.ExecutionListener;
import io.mohs.core.event.Started;
import io.mohs.core.event.Succeeded;
import io.mohs.core.job.JobKey;
import io.mohs.core.resource.ExecutionWindow;
import io.mohs.core.resource.MohsRunner;
import io.mohs.engine.HandlerRegistry;

import static org.assertj.core.api.Assertions.assertThat;

class MohsAutoConfigurationTest {

    record Handler() {
    }

    record Greeting(String name) {
    }

    static class GreetingJob {
        final List<Greeting> received = new CopyOnWriteArrayList<>();

        @MohsJob(id = "greet-annotated")
        void greet(Greeting payload) {
            received.add(payload);
        }
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

    /** Sem HandlerRegistry.register/Mohs.define manual nenhum — prova que MohsJobScanner faz isso sozinho. */
    @Test
    void mohsJobAnnotatedBeanIsScannedAndDispatchedAutomatically() {
        CountDownLatch succeeded = new CountDownLatch(1);
        ExecutionListener listener = event -> {
            if (event instanceof Succeeded) {
                succeeded.countDown();
            }
        };

        runnerWith(freshH2DataSource())
                .withBean(ExecutionListener.class, () -> listener)
                .withUserConfiguration(GreetingJob.class)
                .run(context -> {
                    Mohs mohs = context.getBean(Mohs.class);
                    mohs.schedule("greet-annotated", new Greeting("ana")).now();

                    assertThat(succeeded.await(5, TimeUnit.SECONDS)).as("execution claimed, dispatched and succeeded within timeout").isTrue();
                    assertThat(context.getBean(GreetingJob.class).received).containsExactly(new Greeting("ana"));
                });
    }

    /** AopUtils.getTargetClass/selectInvocableMethod: o método anotado precisa ser achado atrás de um proxy CGLIB, não só na classe crua. */
    @Test
    void mohsJobIsFoundBehindACglibProxy() {
        CountDownLatch succeeded = new CountDownLatch(1);
        ExecutionListener listener = event -> {
            if (event instanceof Succeeded) {
                succeeded.countDown();
            }
        };
        GreetingJob target = new GreetingJob();
        ProxyFactory proxyFactory = new ProxyFactory(target);
        proxyFactory.setProxyTargetClass(true);
        GreetingJob proxy = (GreetingJob) proxyFactory.getProxy();

        runnerWith(freshH2DataSource())
                .withBean(ExecutionListener.class, () -> listener)
                .withBean("greetingJob", GreetingJob.class, () -> proxy)
                .run(context -> {
                    Mohs mohs = context.getBean(Mohs.class);
                    mohs.schedule("greet-annotated", new Greeting("ana")).now();

                    assertThat(succeeded.await(5, TimeUnit.SECONDS)).as("execution claimed, dispatched and succeeded within timeout").isTrue();
                    assertThat(target.received).containsExactly(new Greeting("ana"));
                });
    }

    @Test
    void propertyDefinedRunnerIsResolvable() {
        CountDownLatch succeeded = new CountDownLatch(1);
        ExecutionListener listener = event -> {
            if (event instanceof Succeeded) {
                succeeded.countDown();
            }
        };
        AtomicReference<String> dispatchThreadName = new AtomicReference<>();

        runnerWith(freshH2DataSource(), "mohs.runners.s3.mode=io", "mohs.runners.s3.max=4")
                .withBean(ExecutionListener.class, () -> listener)
                .run(context -> {
                    context.getBean(HandlerRegistry.class).register(JobKey.of("upload"),
                            (payload, ctx) -> dispatchThreadName.set(Thread.currentThread().getName()));

                    Mohs mohs = context.getBean(Mohs.class);
                    mohs.define(JobDefinition.of("upload", Handler.class, spec -> spec.onDemand().runner("s3")));
                    mohs.schedule("upload", new Greeting("ana")).now();

                    assertThat(succeeded.await(5, TimeUnit.SECONDS)).as("execution claimed, dispatched and succeeded within timeout").isTrue();
                });
        assertThat(dispatchThreadName.get()).startsWith("mohs-runner-s3-");
    }

    @Test
    void beanDeclaredRunnerIsCollected() {
        CountDownLatch succeeded = new CountDownLatch(1);
        ExecutionListener listener = event -> {
            if (event instanceof Succeeded) {
                succeeded.countDown();
            }
        };
        AtomicReference<String> dispatchThreadName = new AtomicReference<>();

        runnerWith(freshH2DataSource())
                .withBean(ExecutionListener.class, () -> listener)
                .withBean("batchRunner", MohsRunner.class, () -> MohsRunner.cpu("batch").coreSize(1).maxSize(1).build())
                .run(context -> {
                    context.getBean(HandlerRegistry.class).register(JobKey.of("report"),
                            (payload, ctx) -> dispatchThreadName.set(Thread.currentThread().getName()));

                    Mohs mohs = context.getBean(Mohs.class);
                    mohs.define(JobDefinition.of("report", Handler.class, spec -> spec.onDemand().runner("batch")));
                    mohs.schedule("report", new Greeting("ana")).now();

                    assertThat(succeeded.await(5, TimeUnit.SECONDS)).as("execution claimed, dispatched and succeeded within timeout").isTrue();
                });
        assertThat(dispatchThreadName.get()).startsWith("mohs-runner-batch-");
    }

    @Test
    void duplicateRunnerNameBetweenPropertyAndBeanFailsBoot() {
        runnerWith(freshH2DataSource(), "mohs.runners.batch.mode=cpu")
                .withBean("batchRunner", MohsRunner.class, () -> MohsRunner.cpu("batch").build())
                .run(context -> assertThat(context).hasFailed());
    }

    @Test
    void jobIsNeverDispatchedInsideAnExcludedWindow() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        ExecutionListener listener = event -> {
            if (event instanceof Started) {
                started.countDown();
            }
        };
        ExecutionWindow maintenance = ExecutionWindow.named("maintenance").exclude(instant -> true).build();

        runnerWith(freshH2DataSource())
                .withBean(ExecutionListener.class, () -> listener)
                .withBean("maintenanceWindow", ExecutionWindow.class, () -> maintenance)
                .run(context -> {
                    context.getBean(HandlerRegistry.class).register(JobKey.of("blocked"), (payload, ctx) -> { });

                    Mohs mohs = context.getBean(Mohs.class);
                    mohs.define(JobDefinition.of("blocked", Handler.class, spec -> spec.onDemand().window("maintenance")));
                    mohs.schedule("blocked", new Greeting("ana")).now();

                    assertThat(started.await(300, TimeUnit.MILLISECONDS)).as("never claimed/dispatched while the window excludes now").isFalse();
                });
    }

    @Test
    void jobRunsWhenTheWindowDoesNotExclude() {
        CountDownLatch succeeded = new CountDownLatch(1);
        ExecutionListener listener = event -> {
            if (event instanceof Succeeded) {
                succeeded.countDown();
            }
        };
        ExecutionWindow openWindow = ExecutionWindow.named("maintenance").exclude(instant -> false).build();

        runnerWith(freshH2DataSource())
                .withBean(ExecutionListener.class, () -> listener)
                .withBean("maintenanceWindow", ExecutionWindow.class, () -> openWindow)
                .run(context -> {
                    context.getBean(HandlerRegistry.class).register(JobKey.of("allowed"), (payload, ctx) -> { });

                    Mohs mohs = context.getBean(Mohs.class);
                    mohs.define(JobDefinition.of("allowed", Handler.class, spec -> spec.onDemand().window("maintenance")));
                    mohs.schedule("allowed", new Greeting("ana")).now();

                    assertThat(succeeded.await(5, TimeUnit.SECONDS)).as("execution claimed, dispatched and succeeded within timeout").isTrue();
                });
    }

    /** core-size é campo de CPU e o mode default é io: erro de boot apontando a propriedade, nunca descarte silencioso (que viraria runner IO de 64 threads pra trabalho CPU-bound). */
    @Test
    void wrongModeRunnerPropertyFailsBootNamingTheProperty() {
        runnerWith(freshH2DataSource(), "mohs.runners.batch.core-size=2")
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure()).hasStackTraceContaining("mohs.runners.batch.core-size");
                });
    }

    /** A IAE do builder ("maxSize must be >= coreSize") sozinha não diz qual runner nem qual propriedade — e core-size default depende da máquina; o embrulho dá o contexto. */
    @Test
    void invalidRunnerPropertyFailsBootNamingTheRunner() {
        runnerWith(freshH2DataSource(), "mohs.runners.batch.mode=cpu", "mohs.runners.batch.core-size=4", "mohs.runners.batch.max-size=2")
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure())
                            .hasStackTraceContaining("mohs.runners.batch")
                            .hasStackTraceContaining("maxSize must be >= coreSize");
                });
    }
}
