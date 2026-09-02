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

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import javax.sql.DataSource;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;

import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.beans.factory.NoUniqueBeanDefinitionException;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.context.annotation.Bean;
import org.springframework.boot.autoconfigure.task.TaskExecutionAutoConfiguration;
import org.springframework.boot.autoconfigure.task.TaskSchedulingAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.scheduling.annotation.EnableScheduling;

import io.mohs.core.EngineState;
import io.mohs.core.Mohs;
import io.mohs.core.definition.JobDefinition;
import io.mohs.core.definition.MohsJob;
import io.mohs.core.event.ExecutionEventType;
import io.mohs.core.event.ExecutionListener;
import io.mohs.core.event.Failed;
import io.mohs.core.event.OnExecution;
import io.mohs.core.execution.RetryPolicy;
import io.mohs.core.event.Started;
import io.mohs.core.event.Succeeded;
import io.mohs.core.job.JobKey;
import io.mohs.core.resource.ExecutionWindow;
import io.mohs.core.resource.MohsRunner;
import io.mohs.engine.HandlerRegistry;
import io.mohs.store.jdbc.DatabaseClock;
import io.mohs.store.jdbc.JdbcJobStore;
import io.mohs.store.jdbc.delegate.H2JdbcDelegate;
import io.mohs.store.jdbc.delegate.JdbcDelegate;
import io.mohs.test.MutableClock;

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
        // Applied BEFORE registering the bean in the context — the Engine's SmartLifecycle.start()
        // fires at the end of refresh(), so the schema has to exist before that.
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

                    // Wait INSIDE run(): the context runner closes the context (and the Engine,
                    // through SmartLifecycle.stop) as soon as the lambda returns — claim and
                    // dispatch happen in the background, on a future tick of the poll loop.
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

    /** In {@code database} mode the database is the time authority, so the engine's Clock must be the {@link DatabaseClock}, with the resync scheduler present. */
    @Test
    void databaseTimeModeWiresDatabaseClockAndResyncScheduler() {
        runnerWith(freshH2DataSource(), "mohs.time.mode=database").run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context.getBean("mohsClock", Clock.class)).isInstanceOf(DatabaseClock.class);
            assertThat(context).hasBean("mohsClockSyncScheduler");
        });
    }

    /**
     * The combination that used to be refused at boot. It was refused because {@code CURRENT_TIMESTAMP}
     * is zoneless on both MySQL and SQL Server, and the offset sampled from it was the distance between
     * two zones; now each asks for UTC explicitly and reads the answer as a {@code LocalDateTime}
     * stated to be UTC, so there is nothing left to refuse and {@code mohs.time.mode=application} is a
     * preference rather than the only way to boot.
     *
     * <p>The boot still stops here, and the reason is now the honest one. The DataSource stays H2 while
     * the dialect is declared, so {@code SELECT UTC_TIMESTAMP(6)} does not even parse: the FIRST clock
     * sample fails. What used to be refused for being a zoneless dialect is now refused for the only
     * thing that actually matters — the engine would otherwise start on the local clock the operator
     * said not to trust, with one WARN as the only sign. That the offset is real on a server of the
     * declared kind is {@code DatabaseClockZoneTest}'s subject, against real containers.
     *
     * <p>MySQL only, though the zoneless twins were both opened: SQL Server's boot now stops one bean
     * earlier, at the dialect's RCSI requirement — the test below.
     */
    @Test
    void databaseTimeModeStopsTheBootWhenTheFirstSampleFails() {
        runnerWith(freshH2DataSource(), "mohs.time.mode=database", "mohs.jdbc.dialect=mysql").run(context -> {
            assertThat(context).hasFailed();
            assertThat(context.getStartupFailure())
                    .rootCause()
                    .hasMessageContaining("the first clock sample against the database failed")
                    .hasMessageContaining("mohs.time.mode=application");
        });
    }

    /**
     * The SQL Server dialect's boot requirement, seen from the starter: selecting {@code sqlserver}
     * inspects {@code READ_COMMITTED_SNAPSHOT} before anything else touches the database, and a boot
     * that cannot confirm the setting stops naming it. The DataSource here is H2, so the inspection
     * itself fails — which is the same refusal an unreachable SQL Server earns. That a real server
     * with the setting OFF is refused with the {@code ALTER DATABASE} to run is
     * {@code SqlServerRcsiRequirementTest}'s subject, against a real container.
     */
    @Test
    void sqlServerDialectStopsTheBootWhenRcsiCannotBeConfirmed() {
        runnerWith(freshH2DataSource(), "mohs.jdbc.dialect=sqlserver").run(context -> {
            assertThat(context).hasFailed();
            // Not rootCause(): the refusal deliberately keeps the SQLException as its cause, so the
            // deepest exception is the driver's, and the named refusal sits one level above it.
            assertThat(context.getStartupFailure())
                    .hasStackTraceContaining("could not inspect READ_COMMITTED_SNAPSHOT");
        });
    }

    /**
     * A delegate this repository does not ship. It is the whole point of {@code @ConditionalOnMissingBean}
     * on {@code mohsJdbcDelegate}: a database Mohs has never heard of is served by a bean, not by adding a
     * constant to an enum here — and the property that is otherwise mandatory becomes unnecessary,
     * because there is nothing left for it to select.
     */
    @Test
    void aDelegateBeanWinsOverTheEnumAndMakesThePropertyUnnecessary() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(MohsAutoConfiguration.class))
                .withBean(DataSource.class, MohsAutoConfigurationTest::freshH2DataSource)
                .withBean(JdbcDelegate.class, CommunityDelegate::new)
                .withPropertyValues("mohs.engine.poll-interval=50ms")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context.getBean(JdbcDelegate.class)).isInstanceOf(CommunityDelegate.class);
                });
    }

    /**
     * Database time reaches a delegate written elsewhere and boots on it. This used to be a refusal:
     * the mode was gated on a boolean asking whether {@code CURRENT_TIMESTAMP} carried a zone, and
     * anything that did not answer — a third-party delegate most of all — was turned away. The gate is
     * gone because what it guarded is gone: the statement now asks for UTC explicitly where the server
     * is zoneless, and the crossing back is abstract, so a delegate cannot inherit someone else's
     * answer by saying nothing. {@link CommunityDelegate} supplies both halves and gets a clock.
     *
     * <p>What the offset is worth is not this test's question — it needs a server in another zone, and
     * that is {@code DatabaseClockZoneTest}, against real containers. Here the property is narrower and
     * still worth holding: the mode is no longer closed to delegates this repository did not write.
     */
    @Test
    void databaseTimeModeBootsForADelegateWrittenElsewhere() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(MohsAutoConfiguration.class))
                .withBean(DataSource.class, MohsAutoConfigurationTest::freshH2DataSource)
                .withBean(JdbcDelegate.class, CommunityDelegate::new)
                .withPropertyValues("mohs.engine.poll-interval=50ms", "mohs.time.mode=database")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context.getBean("mohsClock", Clock.class)).isInstanceOf(DatabaseClock.class);
                });
    }

    /**
     * The case {@code @ConditionalOnMissingBean} cannot cover, and the reason every injection point of
     * the delegate is named {@code delegate} rather than {@code mohsJdbcDelegate}.
     *
     * <p>A community delegate shipped in its OWN auto-configuration is ordered against this one by
     * Spring, not by the host — order it after and the condition has already passed, so both delegates
     * reach the context. With the injection points carrying the bean's own name, Spring would resolve
     * that ambiguity by name and silently pick the built-in, running its SQL
     * instead of the substitute's. Failing the boot is the only honest answer.
     *
     * <p>This test is the net under a naming convention that nothing else enforces: rename those five
     * parameters back "for consistency" and the silent wrong choice returns with a green suite.
     */
    @Test
    void twoDelegatesFailTheBootRatherThanLettingTheBuiltInWinSilently() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(MohsAutoConfiguration.class, LateDelegateAutoConfiguration.class))
                .withBean(DataSource.class, MohsAutoConfigurationTest::freshH2DataSource)
                .withPropertyValues("mohs.jdbc.dialect=h2", "mohs.engine.poll-interval=50ms")
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure())
                            .as("two JdbcDelegate candidates must be an error, never a silent choice")
                            .hasRootCauseInstanceOf(NoUniqueBeanDefinitionException.class);
                });
    }

    /**
     * The convention asserted directly, because the context test above cannot see the case that
     * actually costs something.
     *
     * <p>Singletons are pre-instantiated in declaration order, so the FIRST bean to resolve a delegate
     * decides which one every bean after it sees. Rename only that parameter back and Spring resolves
     * it by name: the bean is built with the BUILT-IN delegate, it issues the built-in's SQL against a
     * schema installed for another database, and only the NEXT bean raises the ambiguity that fails
     * the boot.
     *
     * <p>A context test observes the END of the boot; that substitution happens in the middle. So
     * assert the invariant itself: none of these parameters may carry the bean's name.
     */
    @Test
    void noDelegateInjectionPointIsNamedAfterTheDelegateBean() {
        List<String> inspected = new ArrayList<>();
        // Every nested @Configuration, discovered rather than listed: a third one added later would
        // otherwise slip past the rule while the floor below stayed satisfied by the existing ten
        List<Class<?>> configurations = Stream.concat(Stream.of(MohsAutoConfiguration.class),
                Arrays.stream(MohsAutoConfiguration.class.getDeclaredClasses())).toList();
        for (Class<?> configuration : configurations) {
            for (Method beanMethod : configuration.getDeclaredMethods()) {
                if (!beanMethod.isAnnotationPresent(Bean.class)) {
                    continue;
                }
                for (Parameter parameter : beanMethod.getParameters()) {
                    if (parameter.getType() != JdbcDelegate.class) {
                        continue;
                    }
                    inspected.add(configuration.getSimpleName() + "#" + beanMethod.getName());
                    // Without -parameters the compiler emits arg0/arg1 and every name below passes
                    // vacuously — the convention this test defends would be gone with a green suite
                    assertThat(parameter.isNamePresent())
                            .as("%s#%s: parameter names were not compiled in (-parameters is off), so this "
                                    + "rule cannot see the name it exists to check",
                                    configuration.getSimpleName(), beanMethod.getName())
                            .isTrue();
                    assertThat(parameter.getName())
                            .as("%s#%s: a JdbcDelegate parameter named after the bean lets Spring resolve two "
                                    + "candidates by name instead of failing the boot",
                                    configuration.getSimpleName(), beanMethod.getName())
                            .isNotEqualTo("mohsJdbcDelegate");
                }
            }
        }
        // A net has to prove it caught something: a @Bean moved into another nested @Configuration
        // would empty this loop, and the rule would keep passing over nothing at all. Nine today —
        // eight here plus DatabaseTimeConfiguration#mohsClock; the tenth JdbcDelegate parameter in
        // the file belongs to a private static helper, which is not an injection point. It was ten
        // until mohsFlyway went away with the migrations, and this floor is what noticed
        assertThat(inspected).as("no JdbcDelegate injection point was inspected — the scan found nothing to check")
                .hasSizeGreaterThanOrEqualTo(9);
    }

    /** Ordered AFTER Mohs on purpose: that is what makes the condition miss it and both beans coexist. */
    @AutoConfiguration(after = MohsAutoConfiguration.class)
    static class LateDelegateAutoConfiguration {

        @Bean
        JdbcDelegate lateCommunityDelegate() {
            return new CommunityDelegate();
        }
    }

    /** The default ({@code application}): the system clock and no resync bean — the conditional scheduler must not exist outside database mode. */
    @Test
    void defaultTimeModeUsesSystemClockWithoutResyncScheduler() {
        runnerWith(freshH2DataSource()).run(context -> {
            assertThat(context.getBean("mohsClock", Clock.class)).isEqualTo(Clock.systemUTC());
            assertThat(context).doesNotHaveBean("mohsClockSyncScheduler");
        });
    }

    /** No manual HandlerRegistry.register or Mohs.define at all — proof that MohsJobScanner does it by itself. */
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

    /** AopUtils.getTargetClass/selectInvocableMethod: the annotated method must be found behind a CGLIB proxy, not only on the raw class. */
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

    @EnableScheduling
    static class SchedulingEnabledApp {
    }

    static class ObservingGreetingJob {
        final List<Greeting> received = new CopyOnWriteArrayList<>();
        final List<Succeeded> observed = new CopyOnWriteArrayList<>();
        final CountDownLatch observedOnce = new CountDownLatch(1);

        @MohsJob(id = "greet-annotated")
        void greet(Greeting payload) {
            received.add(payload);
        }

        @OnExecution(job = "greet-annotated", event = ExecutionEventType.SUCCEEDED)
        void onSucceeded(Succeeded event) {
            observed.add(event);
            observedOnce.countDown();
        }

        @OnExecution(job = "another-job", event = ExecutionEventType.SUCCEEDED)
        void neverCalled(Succeeded event) {
            observed.add(event);
        }
    }

    /**
     * The annotated method IS a listener — same asynchronous, best-effort delivery — and the job
     * filter is what distinguishes it from one: the second method observes a different job and must
     * stay untouched by this execution.
     */
    @Test
    void onExecutionMethodsReceiveTheirFilteredEvent() {
        runnerWith(freshH2DataSource())
                .withUserConfiguration(ObservingGreetingJob.class)
                .run(context -> {
                    ObservingGreetingJob bean = context.getBean(ObservingGreetingJob.class);
                    context.getBean(Mohs.class).schedule("greet-annotated", new Greeting("ana")).now();

                    assertThat(bean.observedOnce.await(5, TimeUnit.SECONDS))
                            .as("the @OnExecution method received the Succeeded event within the timeout").isTrue();
                    assertThat(bean.observed).singleElement()
                            .satisfies(event -> assertThat(event.jobKey().value()).isEqualTo("greet-annotated"));
                });
    }

    static class MixedObservers {

        /**
         * Both count down BEFORE throwing, and both throw. Subscriptions are held in registration
         * order, which comes from {@code Class#getDeclaredMethods} — order the JLS does not specify.
         * With one observer throwing and one surviving, whichever reflection happened to put first
         * would decide the verdict, and the survivor running first would make the test pass with the
         * per-subscription catch REMOVED. Two throwers have no such permutation: without the catch,
         * the first throw ends the fan-out and the latch never reaches zero.
         */
        final CountDownLatch bothObserversRan = new CountDownLatch(2);

        @MohsJob(id = "greet-annotated")
        void greet(Greeting payload) {
        }

        @OnExecution(event = ExecutionEventType.SUCCEEDED)
        void oneThrows() {
            bothObserversRan.countDown();
            throw new IllegalStateException("observer bug");
        }

        @OnExecution(event = ExecutionEventType.SUCCEEDED)
        void theOtherThrowsToo() {
            bothObserversRan.countDown();
            throw new IllegalStateException("another observer bug");
        }
    }

    /**
     * The property that justifies the single-listener fan-out: N annotated methods share one listener
     * task, so one of them throwing must not cost the others their delivery. It also pins the two
     * forms the other observer test does not reach — the empty job filter, which observes every job,
     * and the no-parameter method: neither observer declares a job or a parameter, so reaching the
     * latch at all is the routing working.
     */
    @Test
    void aThrowingObserverDoesNotSilenceTheOthersAndAnEmptyFilterObservesEveryJob() {
        runnerWith(freshH2DataSource())
                .withUserConfiguration(MixedObservers.class)
                .run(context -> {
                    MixedObservers bean = context.getBean(MixedObservers.class);
                    context.getBean(Mohs.class).schedule("greet-annotated", new Greeting("ana")).now();

                    assertThat(bean.bothObserversRan.await(5, TimeUnit.SECONDS))
                            .as("both observers ran, even though the first one to run threw")
                            .isTrue();
                });
    }

    static class MistypedObserver {
        @OnExecution(job = "greet-annotated", event = ExecutionEventType.SUCCEEDED)
        void onSucceeded(Failed event) {
        }
    }

    /**
     * A parameter that cannot hold the declared event is a method that would never run — the same
     * class of silent failure the old "not supported yet" rejection existed to prevent, which is why
     * this one still fails the boot.
     */
    @Test
    void anOnExecutionMethodThatCannotReceiveItsEventFailsTheBoot() {
        runnerWith(freshH2DataSource())
                .withUserConfiguration(MistypedObserver.class)
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure())
                            .hasStackTraceContaining("@OnExecution")
                            .hasStackTraceContaining("cannot receive it");
                });
    }

    /**
     * An embedded library must not change the semantics of the host's context merely by being on
     * the classpath: Mohs's beans of generic types (Clock, ThreadPoolTaskScheduler,
     * AsyncTaskExecutor) are {@code defaultCandidate = false}.
     *
     * <p>Without that, MohsAutoConfiguration — ordered before Boot's auto-configurations,
     * alphabetically — suppressed the application's {@code taskScheduler}/
     * {@code applicationTaskExecutor} through {@code @ConditionalOnMissingBean} by type, and the
     * host's {@code @Scheduled} silently fell back to serial execution.
     */
    @Test
    void mohsBeansDoNotSuppressTheHostTaskSchedulerAndExecutor() {
        DataSource dataSource = freshH2DataSource();
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(
                        TaskSchedulingAutoConfiguration.class, TaskExecutionAutoConfiguration.class, MohsAutoConfiguration.class))
                .withBean(DataSource.class, () -> dataSource)
                .withUserConfiguration(SchedulingEnabledApp.class)
                .withPropertyValues("mohs.jdbc.dialect=h2", "mohs.engine.poll-interval=50ms")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).hasBean("taskScheduler");
                    assertThat(context).hasBean("applicationTaskExecutor");
                });
    }

    /**
     * With per-tick lease renewal, a healthy slow handler is no longer reclaimed. The remaining
     * risk is a Watchdog Bound smaller than the job's declared {@code timeout}: the node would stop
     * renewing before the deadline the job gave itself. The operator learns the price at boot (a
     * WARN naming the job and the property), not in the postmortem.
     */
    @Test
    void bootWarnsWhenADeclaredJobTimeoutReachesTheWatchdogBound() {
        DataSource dataSource = freshH2DataSource();
        // A job persisted by an earlier deploy — the WARN runs at engine start, with the store already populated
        new JdbcJobStore(dataSource, new MutableClock(Instant.parse("2026-08-15T12:00:00Z"), ZoneId.of("UTC")), new H2JdbcDelegate())
                .upsert(JobDefinition.of("slow-report", Handler.class, spec -> spec.onDemand().timeout(Duration.ofMinutes(5))));
        ch.qos.logback.classic.Logger lifecycleLogger =
                (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(MohsEngineLifecycle.class);
        ListAppender<ILoggingEvent> warnWatcher = new ListAppender<>();
        warnWatcher.start();
        lifecycleLogger.addAppender(warnWatcher);
        try {
            runnerWith(dataSource, "mohs.engine.watchdog-timeout=2m").run(context -> {
                assertThat(context).hasNotFailed();
                // The SmartLifecycle's start() runs inside the refresh — the WARN has already happened by here
                assertThat(warnWatcher.list).anyMatch(event -> event.getFormattedMessage().contains("slow-report")
                        && event.getFormattedMessage().contains("mohs.engine.watchdog-timeout"));
            });
        } finally {
            lifecycleLogger.detachAppender(warnWatcher);
        }
    }

    /** A bound at or below the NODE's lease is a boot error naming both properties — a bound below it would release ownership before the node could be considered dead. */
    @Test
    void watchdogTimeoutBelowNodeLeaseTtlFailsBoot() {
        runnerWith(freshH2DataSource(), "mohs.engine.node-lease-ttl=15s", "mohs.engine.watchdog-timeout=10s")
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure())
                            .hasStackTraceContaining("mohs.engine.watchdog-timeout")
                            .hasStackTraceContaining("mohs.engine.node-lease-ttl");
                });
    }

    /**
     * One second below the floor, so the pair with {@link #nodeLeaseTtlAtTheFloorBoots} pins the
     * constant instead of merely proving the check exists. Eleven seconds is not absurd — it clears the
     * healthy-tick budget by a third of a second — and that is the point: the floor is chosen by
     * margin, so the value it has to reject is the one right underneath it. The message has to carry
     * the floor, not just the property name; it is what the operator types.
     */
    @Test
    void nodeLeaseTtlJustBelowTheFloorFailsBoot() {
        runnerWith(freshH2DataSource(), "mohs.engine.node-lease-ttl=11s")
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure())
                            .hasStackTraceContaining("mohs.engine.node-lease-ttl")
                            .hasStackTraceContaining("12s");
                });
    }

    /** The floor itself boots — the rejection is of what is below it, not of everything short of the default. */
    @Test
    void nodeLeaseTtlAtTheFloorBoots() {
        runnerWith(freshH2DataSource(), "mohs.engine.node-lease-ttl=12s")
                .run(context -> assertThat(context).hasNotFailed());
    }

    /** Retry end to end: the 1st attempt fails, the execution comes back as RETRY_WAITING with backoff, the same claim path picks it up again and the 2nd succeeds. */
    @Test
    void failedExecutionIsRetriedThroughTheRealEngineUntilItSucceeds() {
        CountDownLatch succeeded = new CountDownLatch(1);
        AtomicInteger attempts = new AtomicInteger();
        ExecutionListener listener = event -> {
            if (event instanceof Succeeded) {
                succeeded.countDown();
            }
        };

        runnerWith(freshH2DataSource())
                .withBean(ExecutionListener.class, () -> listener)
                .run(context -> {
                    context.getBean(HandlerRegistry.class).register(JobKey.of("flaky"), (payload, ctx) -> {
                        if (attempts.incrementAndGet() == 1) {
                            throw new IllegalStateException("first attempt fails");
                        }
                    });

                    Mohs mohs = context.getBean(Mohs.class);
                    mohs.define(JobDefinition.of("flaky", Handler.class, spec -> spec.onDemand().retries(2).runner("io")));
                    mohs.schedule("flaky", new Greeting("ana")).now();

                    assertThat(succeeded.await(10, TimeUnit.SECONDS)).as("retried and succeeded within timeout").isTrue();
                    assertThat(attempts.get()).isEqualTo(2);
                });
    }

    /**
     * A job naming a policy bean that does not exist fails the boot: the alternative is an execution
     * that fails with the built-in backoff while the operator believes a custom policy chose it —
     * the same silent-gap argument that used to justify the WARN, now that there is a real SPI to
     * point at.
     */
    @Test
    void bootFailsWhenAJobNamesARetryPolicyBeanThatDoesNotExist() {
        DataSource dataSource = freshH2DataSource();
        new JdbcJobStore(dataSource, new MutableClock(Instant.parse("2026-08-15T12:00:00Z"), ZoneId.of("UTC")), new H2JdbcDelegate())
                .upsert(JobDefinition.of("flaky-report", Handler.class, spec -> spec.onDemand().retryPolicy("myRetryPolicyBean")));

        runnerWith(dataSource).run(context -> {
            assertThat(context).hasFailed();
            assertThat(context.getStartupFailure())
                    .hasStackTraceContaining("flaky-report")
                    .hasStackTraceContaining("myRetryPolicyBean");
        });
    }

    /**
     * The whole point of the SPI: the delay comes from the bean, not from the built-in backoff. The
     * policy answers a fixed, tiny delay — the assertion is that the retry happened at all and that
     * the policy was the one asked, which the recorded failures prove.
     */
    @Test
    void aCustomRetryPolicyDecidesWhenTheNextAttemptRuns() {
        List<RetryPolicy.Failure> consulted = new CopyOnWriteArrayList<>();
        AtomicInteger attempts = new AtomicInteger();
        CountDownLatch succeeded = new CountDownLatch(1);
        ExecutionListener listener = event -> {
            if (event instanceof Succeeded) {
                succeeded.countDown();
            }
        };

        runnerWith(freshH2DataSource())
                .withBean(ExecutionListener.class, () -> listener)
                .withBean("stubbornRetries", RetryPolicy.class, () -> failure -> {
                    consulted.add(failure);
                    return Optional.of(Duration.ofMillis(50));
                })
                .run(context -> {
                    Mohs mohs = context.getBean(Mohs.class);
                    context.getBean(HandlerRegistry.class).register(JobKey.of("stubborn"), (payload, ctx) -> {
                        // retries(0) would be terminal on the built-in schedule — the policy is what keeps it alive
                        if (attempts.incrementAndGet() == 1) {
                            throw new IllegalStateException("first attempt always fails");
                        }
                    });
                    mohs.define(JobDefinition.of("stubborn", Handler.class,
                            spec -> spec.onDemand().retries(0).retryPolicy("stubbornRetries").runner("io")));
                    mohs.schedule("stubborn", new Greeting("ana")).now();

                    assertThat(succeeded.await(10, TimeUnit.SECONDS))
                            .as("the policy granted a retry that the declared budget of 0 would have refused").isTrue();
                    assertThat(attempts.get()).isEqualTo(2);
                    assertThat(consulted).singleElement().satisfies(failure -> {
                        assertThat(failure.jobKey().value()).isEqualTo("stubborn");
                        assertThat(failure.failedAttempt()).isEqualTo(1);
                        assertThat(failure.retries()).isZero();
                        assertThat(failure.error()).isInstanceOf(IllegalStateException.class);
                    });
                });
    }
}
