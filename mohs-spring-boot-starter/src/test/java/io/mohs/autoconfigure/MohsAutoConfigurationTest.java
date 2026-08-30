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

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import javax.sql.DataSource;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;

import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.LoggerFactory;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.boot.autoconfigure.AutoConfigurations;
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
     * The combination that used to boot and then answer the wrong time. The dialect is declared while
     * the DataSource stays H2 — the guard runs before a single query is issued, so the test needs
     * neither engine, and the ONE thing it must prove is that the boot stops instead of proceeding
     * with an offset that is really a zone difference.
     *
     * <p>Both dialects, because a guard that closes one trap and leaves its twin open is worse than
     * none: it says the subject was handled.
     */
    @ParameterizedTest
    @ValueSource(strings = {"sqlserver", "mysql"})
    void databaseTimeModeOnAZonelessDialectFailsTheBootNamingTheAlternative(String dialect) {
        runnerWith(freshH2DataSource(), "mohs.time.mode=database", "mohs.jdbc.dialect=" + dialect).run(context -> {
            assertThat(context).hasFailed();
            assertThat(context.getStartupFailure())
                    .rootCause()
                    .hasMessageContaining("mohs.time.mode=database is not supported on")
                    .hasMessageContaining("mohs.time.mode=application");
        });
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
        new JdbcJobStore(dataSource, new MutableClock(Instant.parse("2026-08-15T12:00:00Z"), ZoneId.of("UTC")))
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
        new JdbcJobStore(dataSource, new MutableClock(Instant.parse("2026-08-15T12:00:00Z"), ZoneId.of("UTC")))
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
