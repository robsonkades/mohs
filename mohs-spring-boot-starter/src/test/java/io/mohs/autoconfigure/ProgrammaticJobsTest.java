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
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicInteger;
import org.aopalliance.intercept.MethodInterceptor;
import org.springframework.aop.framework.ProxyFactory;
import javax.sql.DataSource;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.transaction.support.TransactionTemplate;
import io.mohs.core.Mohs;
import io.mohs.core.SubmissionOptions;
import io.mohs.core.definition.JobOptions;
import io.mohs.core.definition.JobRegistration;
import io.mohs.core.definition.OnDemandJob;
import io.mohs.core.event.Enqueued;
import io.mohs.core.execution.ExecutionState;
import io.mohs.core.job.JobKey;
import io.mohs.core.job.JobRef;
import io.mohs.core.resource.RateLimit;
import io.mohs.core.schedule.CronSpec;
import io.mohs.core.schedule.IntervalSpec;
import io.mohs.core.schedule.OnDemandSpec;
import io.mohs.core.schedule.RecurringOptions;
import io.mohs.engine.HandlerRegistry;
import io.mohs.engine.JobStore;
import io.mohs.engine.HistoryStore;

import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

class ProgrammaticJobsTest {
    static final JobRef<TaxInput> TAX = JobRef.of("calculate-tax", TaxInput.class);
    public record TaxInput(String company, int year) {}
    public record MutableInput(List<String> companies) {}

    static class Calculator {
        final List<TaxInput> received = new CopyOnWriteArrayList<>();
        public void calculate(TaxInput input) { received.add(input); }
    }

    @Configuration(proxyBeanMethods = false)
    static class Jobs {
        @Bean Calculator calculator() { return new Calculator(); }
        @Bean JobRegistration<TaxInput> tax(Calculator calculator) {
            return JobRegistration.of(TAX, (input, context) -> calculator.calculate(input))
                    .withOptions(JobOptions.defaults().withName("Calculate tax").withRetries(2));
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class RateLimitedJobs {
        final Mohs mohs;
        final List<TaxInput> received = new CopyOnWriteArrayList<>();
        final AtomicInteger notifications = new AtomicInteger();

        RateLimitedJobs(Mohs mohs) { this.mohs = mohs; }

        @Bean RateLimit taxRateLimit() { return new RateLimit("tax", 100, Duration.ofMinutes(1)); }

        @Bean JobRegistration<TaxInput> tax() {
            return JobRegistration.of(TAX, (input, context) -> received.add(input))
                    .withOptions(JobOptions.defaults().withRateLimit("tax"));
        }

        @OnDemandJob(id = "notify-tax", rateLimit = "tax")
        void notifyTax() { notifications.incrementAndGet(); }
    }

    @Test
    void rateLimitConfigurationCanInjectMohsAndDeclareBothKindsOfHandler() {
        baseRunner(database()).withAllowCircularReferences(false)
                .withUserConfiguration(RateLimitedJobs.class).run(context -> {
            assertThat(context).hasNotFailed();
            RateLimitedJobs jobs = context.getBean(RateLimitedJobs.class);
            Mohs mohs = context.getBean(Mohs.class);
            assertThat(jobs.mohs).isSameAs(mohs);
            assertThat(mohs.rateLimits()).extracting(limit -> limit.rateLimit().name()).containsExactly("tax");

            Enqueued tax = mohs.enqueue(TAX, new TaxInput("A", 2026));
            Enqueued notification = mohs.enqueue(JobRef.of("notify-tax", io.mohs.core.NoInput.class), io.mohs.core.NoInput.INSTANCE);
            mohs.lifecycle().start();
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
                assertThat(mohs.findExecution(tax.executionId()).orElseThrow().state()).isEqualTo(ExecutionState.SUCCEEDED);
                assertThat(mohs.findExecution(notification.executionId()).orElseThrow().state()).isEqualTo(ExecutionState.SUCCEEDED);
                assertThat(jobs.received).containsExactly(new TaxInput("A", 2026));
                assertThat(jobs.notifications.get()).isEqualTo(1);
            });
        });
    }

    static DataSource database() {
        JdbcDataSource ds = new JdbcDataSource();
        ds.setURL("jdbc:h2:mem:programmatic-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        ds.setUser("sa");
        new ResourceDatabasePopulator(new ClassPathResource("schema-h2.sql")).execute(ds);
        return ds;
    }

    static ApplicationContextRunner runner(DataSource ds) {
        return baseRunner(ds).withUserConfiguration(Jobs.class);
    }

    static ApplicationContextRunner baseRunner(DataSource ds) {
        return new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(MohsAutoConfiguration.class))
                .withBean(DataSource.class, () -> ds)
                .withPropertyValues("mohs.jdbc.dialect=h2", "mohs.lifecycle.start-mode=manual",
                        "mohs.engine.poll-interval=20ms", "mohs.engine.max-poll-interval=100ms");
    }

    interface TaxCalculator { void calculate(TaxInput input); }

    static class ProxyCalls {
        final List<TaxInput> received = new CopyOnWriteArrayList<>();
        final AtomicInteger advised = new AtomicInteger();
    }

    static class LegacyTax {
        private final TaxCalculator calculator;
        LegacyTax(TaxCalculator calculator) { this.calculator = calculator; }
        @OnDemandJob("legacy-tax")
        public void calculate(TaxInput input) { calculator.calculate(input); }
    }

    static class ConflictingAnnotation {
        @OnDemandJob("calculate-tax")
        public void calculate(TaxInput input) {}
    }

    @Configuration(proxyBeanMethods = false)
    static class ProxiedJobs {
        @Bean ProxyCalls calls() { return new ProxyCalls(); }
        @Bean TaxCalculator calculator(ProxyCalls calls) {
            TaxCalculator target = input -> calls.received.add(input);
            ProxyFactory factory = new ProxyFactory(target);
            factory.addAdvice((MethodInterceptor) invocation -> {
                calls.advised.incrementAndGet();
                return invocation.proceed();
            });
            return (TaxCalculator) factory.getProxy();
        }
        @Bean JobRegistration<TaxInput> tax(TaxCalculator calculator) {
            return JobRegistration.of(TAX, (input, context) -> calculator.calculate(input));
        }
        @Bean LegacyTax legacyTax(TaxCalculator calculator) { return new LegacyTax(calculator); }
    }

    @Test
    void registeredActionsKeepSpringAdviceAndReuseAnnotatedExecutables() {
        baseRunner(database()).withUserConfiguration(ProxiedJobs.class).run(context -> {
            assertThat(context).hasNotFailed();
            Mohs mohs = context.getBean(Mohs.class);
            mohs.enqueue(TAX, new TaxInput("A", 2026));
            JobRef<TaxInput> legacy = JobRef.of("legacy-tax", TaxInput.class);
            mohs.enqueue(legacy, new TaxInput("B", 2027));
            mohs.scheduleRecurring("legacy-recurring", legacy, new TaxInput("C", 2028),
                    new IntervalSpec(Duration.ofMillis(100), false));
            mohs.lifecycle().start();
            ProxyCalls calls = context.getBean(ProxyCalls.class);
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
                assertThat(calls.received).contains(new TaxInput("A", 2026), new TaxInput("B", 2027), new TaxInput("C", 2028));
                assertThat(calls.advised.get()).isGreaterThanOrEqualTo(3);
            });
        });
    }

    @Test
    void oneRegistrationExecutesIndependentInputsAndRecurrencesAfterRestart() {
        DataSource ds = database();
        AtomicReference<List<Enqueued>> receipts = new AtomicReference<>();
        runner(ds).run(context -> {
            assertThat(context).hasNotFailed();
            Mohs mohs = context.getBean(Mohs.class);
            Clock clock = context.getBean("mohsClock", Clock.class);
            receipts.set(List.of(
                    mohs.enqueue(TAX, new TaxInput("A", 2026)),
                    mohs.enqueue(TAX, new TaxInput("B", 2025)),
                    mohs.scheduleAt(TAX, new TaxInput("A", 2027), clock.instant().plusMillis(200))));
            mohs.scheduleRecurring("tax-a", TAX, new TaxInput("A", 2030),
                    new IntervalSpec(Duration.ofMillis(150), false));
            mohs.scheduleRecurring("tax-b", TAX, new TaxInput("B", 2031),
                    new IntervalSpec(Duration.ofMillis(150), true));
            assertThat(context.getBean(Calculator.class).received).isEmpty();
            assertThat(mohs.jobs()).hasSize(3);
            assertThat(context.getBean(HandlerRegistry.class).find(JobKey.of("tax-a"))).isEmpty();
            assertThat(context.getBean(JobStore.class).bindings()).hasSize(3);
        });
        runner(ds).run(context -> {
            assertThat(context).hasNotFailed();
            Mohs mohs = context.getBean(Mohs.class);
            mohs.lifecycle().start();
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                    assertThat(context.getBean(Calculator.class).received).contains(
                            new TaxInput("A", 2026), new TaxInput("B", 2025), new TaxInput("A", 2027),
                            new TaxInput("A", 2030), new TaxInput("B", 2031)));
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                    receipts.get().forEach(receipt -> assertThat(mohs.findExecution(receipt.executionId())
                            .orElseThrow().state()).isEqualTo(ExecutionState.SUCCEEDED)));
        });
    }

    @Test
    void recurrenceUpdatesPreserveIdentityPauseAndAlreadySubmittedInputs() {
        runner(database()).run(context -> {
            Mohs mohs = context.getBean(Mohs.class);
            var cron = new CronSpec("0 0 9 * * MON", ZoneId.of("America/Sao_Paulo"));
            var first = mohs.scheduleRecurring("tax-a", TAX, new TaxInput("A", 2026), cron,
                    RecurringOptions.defaults().withStartPaused(true));
            Enqueued separate = mohs.enqueue(TAX, new TaxInput("A", 2026));
            var updated = mohs.scheduleRecurring("tax-a", TAX, new TaxInput("A", 2027), cron);
            assertThat(updated.key()).isEqualTo(first.key());
            assertThat(updated.nextFireAt()).isEqualTo(first.nextFireAt());
            assertThat(updated.paused()).isTrue();
            assertThat(mohs.jobs()).hasSize(2);
            var binding = context.getBean(JobStore.class).findBinding(updated.key()).orElseThrow();
            assertThat(io.mohs.test.InputFixtures.decode(binding.input(), TaxInput.class))
                    .isEqualTo(new TaxInput("A", 2027));
            mohs.resume(updated.key());
            assertThat(mohs.findJob(updated.key()).orElseThrow().paused()).isFalse();
            mohs.remove(updated.key());
            assertThat(mohs.findJob(updated.key())).isEmpty();
            assertThat(mohs.findExecution(separate.executionId())).isPresent();
            assertThat(mohs.findJob(TAX.key())).isPresent();
        });
    }

    @Test
    void submissionsAndRecurrencesSnapshotMutableInputBeforeReturning() {
        JobRef<MutableInput> ref = JobRef.of("mutable", MutableInput.class);
        runner(database()).withBean("mutable-job", JobRegistration.class,
                () -> JobRegistration.of(ref, (input, context) -> {})).run(context -> {
            Mohs mohs = context.getBean(Mohs.class);
            var companies = new java.util.ArrayList<>(List.of("A"));
            var input = new MutableInput(companies);
            Enqueued receipt = mohs.enqueue(ref, input);
            var schedule = mohs.scheduleRecurring("mutable-recurring", ref, input,
                    new IntervalSpec(Duration.ofHours(1), false));
            companies.add("B");
            HistoryStore history = context.getBean(HistoryStore.class);
            assertThat(io.mohs.test.InputFixtures.decode(history.findPayloads(List.of(receipt.executionId())).rows().get(receipt.executionId()).payload(), MutableInput.class))
                    .isEqualTo(new MutableInput(List.of("A")));
            var binding = context.getBean(JobStore.class).findBinding(schedule.key()).orElseThrow();
            assertThat(io.mohs.test.InputFixtures.decode(binding.input(), MutableInput.class)).isEqualTo(new MutableInput(List.of("A")));
        });
    }

    @Test
    void invalidContractsAndOptionsWriteNothing() {
        DataSource ds = database();
        runner(ds).run(context -> {
            Mohs mohs = context.getBean(Mohs.class);
            TaxInput input = new TaxInput("A", 2026);
            assertThatThrownBy(() -> mohs.enqueue(JobRef.of("missing", TaxInput.class), input))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("JobRegistration");
            assertThatThrownBy(() -> mohs.enqueue(JobRef.of("calculate-tax", String.class), "wrong"))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining(TaxInput.class.getName());
            assertThatThrownBy(() -> mohs.enqueue(io.mohs.core.job.JobRef.of("calculate-tax", String.class), "wrong", io.mohs.core.SubmissionOptions.defaults()))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("expects");
            assertThatThrownBy(() -> mohs.batch("invalid", b -> b.add(JobRef.of("calculate-tax", String.class), "wrong")))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("expects");
            assertThatThrownBy(() -> mohs.scheduleAfter(TAX, input, Duration.ofSeconds(-1)))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("negative");
            assertThatThrownBy(() -> mohs.scheduleAfter(TAX, input, Duration.ofSeconds(Long.MAX_VALUE)))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("range");
            assertThatThrownBy(() -> mohs.scheduleRecurring("tax-a", TAX, input, new OnDemandSpec()))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> mohs.scheduleRecurring("tax-a", TAX, input, new CronSpec("bad", ZoneId.of("UTC"))))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> mohs.scheduleRecurring("calculate-tax", TAX, input,
                    new IntervalSpec(Duration.ofHours(1), false))).isInstanceOf(IllegalArgumentException.class);
            assertThat(new JdbcTemplate(ds).queryForObject("SELECT COUNT(*) FROM mohs_execution", Integer.class)).isZero();
            assertThat(mohs.jobs()).hasSize(1);
        });
    }

    @Test
    void submissionReceiptsDeduplicateAndHostRollbackRemovesAllNewWork() {
        DataSource ds = database();
        runner(ds).run(context -> {
            Mohs mohs = context.getBean(Mohs.class);
            TaxInput input = new TaxInput("A", 2026);
            var options = SubmissionOptions.defaults().withActor("tax-service").withIdempotencyKey("A-2026");
            Enqueued first = mohs.enqueue(TAX, input, options);
            assertThat(mohs.enqueue(TAX, input, options)).isEqualTo(first);
            assertThat(first.actor()).isEqualTo("tax-service");
            AtomicReference<Enqueued> rolledBack = new AtomicReference<>();
            new TransactionTemplate(new DataSourceTransactionManager(ds)).executeWithoutResult(status -> {
                rolledBack.set(mohs.scheduleAfter(TAX, input, Duration.ZERO));
                mohs.scheduleRecurring("rolled-back", TAX, input, new IntervalSpec(Duration.ofHours(1), false));
                status.setRollbackOnly();
            });
            assertThat(mohs.findExecution(rolledBack.get().executionId())).isEmpty();
            assertThat(mohs.findJob(JobKey.of("rolled-back"))).isEmpty();
            assertThat(context.getBean(JobStore.class).findBinding(JobKey.of("rolled-back"))).isEmpty();
        });
    }

    @Test
    void retainedIdempotencyReturnsTheReceiptAfterHistoryPruning() {
        DataSource ds = database();
        runner(ds).run(context -> {
            Mohs mohs = context.getBean(Mohs.class);
            var options = SubmissionOptions.defaults().withIdempotencyKey("A-2026");
            Enqueued first = mohs.enqueue(TAX, new TaxInput("A", 2026), options);
            mohs.cancel(first.executionId());
            new JdbcTemplate(ds).update("DELETE FROM mohs_execution WHERE execution_id = ?", first.executionId().value());
            assertThat(mohs.enqueue(TAX, new TaxInput("A", 2026), options)).isEqualTo(first);
            assertThat(mohs.findExecution(first.executionId())).isEmpty();
        });
    }

    @Test
    void duplicateDeclarationsAndUnknownPolicyFailBeforeWorkersStart() {
        runner(database()).withBean("duplicate", JobRegistration.class,
                () -> JobRegistration.of(TAX, (input, context) -> {})).run(context ->
                assertThat(context).hasFailed());
        runner(database()).withBean("bad-policy", JobRegistration.class,
                () -> JobRegistration.of(JobRef.of("bad", TaxInput.class), (input, context) -> {})
                        .withOptions(JobOptions.defaults().withRunner("missing"))).run(context ->
                assertThat(context).hasFailed());
    }

    @Test
    void unknownRateLimitFailsBeforeDefinitionsArePublishedOrWorkersStart() {
        DataSource ds = database();
        AtomicInteger calls = new AtomicInteger();
        baseRunner(ds).withPropertyValues("mohs.lifecycle.start-mode=auto")
                .withBean(JobRegistration.class, () ->
                        JobRegistration.of(TAX, (input, context) -> calls.incrementAndGet())
                                .withOptions(JobOptions.defaults().withRateLimit("missing")))
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure()).hasStackTraceContaining(
                            "invalid policy for job 'calculate-tax': no declared rate limit named 'missing'");
                });
        assertThat(calls.get()).isZero();
        assertThat(new JdbcTemplate(ds).queryForObject("SELECT COUNT(*) FROM mohs_job_definitions", Integer.class)).isZero();
    }

    @Test
    void propertyOnlyRateLimitCanBeUsedByRegisteredJobsAndTheirRecurrences() {
        baseRunner(database()).withPropertyValues("mohs.rate-limits.tax.max=3", "mohs.rate-limits.tax.window=1m")
                .withBean(JobRegistration.class, () ->
                        JobRegistration.of(TAX, (input, context) -> {})
                                .withOptions(JobOptions.defaults().withRateLimit("tax")))
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    Mohs mohs = context.getBean(Mohs.class);
                    assertThat(mohs.rateLimits()).extracting(limit -> limit.rateLimit())
                            .containsExactly(new RateLimit("tax", 3, Duration.ofMinutes(1)));
                    var schedule = mohs.scheduleRecurring("tax-a", TAX, new TaxInput("A", 2026),
                            new IntervalSpec(Duration.ofDays(1), false));
                    assertThat(mohs.findJob(schedule.key()).orElseThrow().definition().rateLimit()).isEqualTo("tax");
                });
    }

    @Test
    void malformedRateLimitStillFailsStartupWhenNoJobUsesIt() {
        baseRunner(database()).withPropertyValues("mohs.rate-limits.unused.max=3")
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure()).hasStackTraceContaining(
                            "rate limit 'unused' is missing mohs.rate-limits.unused.window");
                });
    }

    @Test
    void missingExecutableOnRestartFailsWithItsDependentScheduleNamed() {
        DataSource ds = database();
        runner(ds).run(context -> context.getBean(Mohs.class).scheduleRecurring(
                "tax-a", TAX, new TaxInput("A", 2026), new IntervalSpec(Duration.ofHours(1), false)));
        new ApplicationContextRunner().withConfiguration(AutoConfigurations.of(MohsAutoConfiguration.class))
                .withBean(DataSource.class, () -> ds)
                .withPropertyValues("mohs.jdbc.dialect=h2", "mohs.lifecycle.start-mode=manual")
                .run(context -> assertThat(context).hasFailed());
    }

    @Test
    void annotationCannotReclaimARetiredExecutableIdentity() {
        DataSource ds = database();
        runner(ds).run(context -> context.getBean(Mohs.class).remove(TAX.key()));
        baseRunner(ds).withBean(ConflictingAnnotation.class, ConflictingAnnotation::new)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context.getBean(Mohs.class).findJob(TAX.key())).isEmpty();
                });
        assertThat(new JdbcTemplate(ds).queryForObject(
                "SELECT source FROM mohs_job_definitions WHERE job_key = ?", String.class, TAX.key().value()))
                .isEqualTo("PROGRAMMATIC");
    }
}
