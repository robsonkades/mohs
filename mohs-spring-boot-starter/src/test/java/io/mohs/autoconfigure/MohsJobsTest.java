package io.mohs.autoconfigure;

import java.io.IOException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.mohs.core.definition.DefinitionSource;
import io.mohs.core.definition.JobDefinition;
import io.mohs.core.definition.MohsJob;
import io.mohs.core.execution.ExecutionId;
import io.mohs.core.execution.JobContext;
import io.mohs.core.job.JobKey;
import io.mohs.core.schedule.CronSpec;
import io.mohs.core.schedule.IntervalSpec;
import io.mohs.core.schedule.Misfire;
import io.mohs.core.schedule.OnDemandSpec;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MohsJobsTest {

    record Greeting(String name) {
    }

    static class Handlers {
        final List<Object> calls = new ArrayList<>();

        void noArgs() {
            calls.add("noArgs");
        }

        void payloadOnly(Greeting payload) {
            calls.add(payload);
        }

        void contextOnly(JobContext ctx) {
            calls.add(ctx);
        }

        void payloadThenContext(Greeting payload, JobContext ctx) {
            calls.add(payload);
            calls.add(ctx);
        }

        void contextThenPayload(JobContext ctx, Greeting payload) {
            calls.add(ctx);
            calls.add(payload);
        }

        void tooManyArgs(Greeting a, JobContext b, Greeting c) {
        }

        void twoPayloads(Greeting a, Greeting b) {
        }

        void twoContexts(JobContext a, JobContext b) {
        }

        void throwsCheckedFromHandler() throws IOException {
            throw new IOException("boom");
        }
    }

    static class AnnotatedFixtures {
        @MohsJob(id = "cron-job", cron = "0 0 2 * * *", zone = "UTC")
        void cronMethod() {
        }

        @MohsJob(id = "every-job", every = "PT30S")
        void everyMethod() {
        }

        @MohsJob(id = "every-after-finish-job", everyAfterFinish = "PT1M")
        void everyAfterFinishMethod() {
        }

        @MohsJob(id = "on-demand-job")
        void onDemandMethod() {
        }

        @MohsJob(id = "conflicting-job", cron = "0 0 2 * * *", zone = "UTC", every = "PT30S")
        void conflictingTriggersMethod() {
        }

        @MohsJob(id = "cron-no-zone-job", cron = "0 0 2 * * *")
        void cronWithoutZoneMethod() {
        }

        @MohsJob(id = "named-job", name = "Friendly Name")
        void namedMethod() {
        }

        @MohsJob(id = "unnamed-job")
        void unnamedMethod() {
        }

        @MohsJob(id = "exclusive-job", allowConcurrentExecutions = false, maxConcurrentExecutions = 3)
        void exclusiveMethod() {
        }

        @MohsJob(id = "invalid-exclusive-job", allowConcurrentExecutions = false)
        void invalidExclusiveMethod() {
        }

        @MohsJob(id = "dormant-job", every = "PT30S", startPaused = true)
        void dormantMethod() {
        }
    }

    private static Method method(String name, Class<?>... paramTypes) {
        try {
            return Handlers.class.getDeclaredMethod(name, paramTypes);
        } catch (NoSuchMethodException e) {
            throw new AssertionError(e);
        }
    }

    private static MohsJob annotationOf(String methodName) {
        try {
            return AnnotatedFixtures.class.getDeclaredMethod(methodName).getAnnotation(MohsJob.class);
        } catch (NoSuchMethodException e) {
            throw new AssertionError(e);
        }
    }

    private static JobContext fakeContext() {
        return new JobContext() {
            @Override
            public JobKey jobKey() {
                return JobKey.of("test");
            }

            @Override
            public ExecutionId executionId() {
                return ExecutionId.of("exec-1");
            }

            @Override
            public int attempt() {
                return 1;
            }

            @Override
            public Instant scheduledAt() {
                return Instant.EPOCH;
            }

            @Override
            public Instant firedAt() {
                return Instant.EPOCH;
            }

            @Override
            public boolean cancellationRequested() {
                return false;
            }

            @Override
            public void progress(int done, int total) {
            }
        };
    }

    // ---- adaptHandler ----

    @Test
    void invokesNoArgMethod() throws Exception {
        Handlers bean = new Handlers();
        MohsJobs.AdaptedHandler adapted = MohsJobs.adaptHandler(bean, method("noArgs"));

        adapted.handler().invoke("payload-ignored", fakeContext());

        assertThat(bean.calls).containsExactly("noArgs");
        assertThat(adapted.payloadType()).isNull();
    }

    @Test
    void invokesPayloadOnlyMethod() throws Exception {
        Handlers bean = new Handlers();
        MohsJobs.AdaptedHandler adapted = MohsJobs.adaptHandler(bean, method("payloadOnly", Greeting.class));
        Greeting payload = new Greeting("ana");

        adapted.handler().invoke(payload, fakeContext());

        assertThat(bean.calls).containsExactly(payload);
        assertThat(adapted.payloadType()).isEqualTo(Greeting.class);
    }

    @Test
    void invokesContextOnlyMethod() throws Exception {
        Handlers bean = new Handlers();
        MohsJobs.AdaptedHandler adapted = MohsJobs.adaptHandler(bean, method("contextOnly", JobContext.class));
        JobContext ctx = fakeContext();

        adapted.handler().invoke("payload-ignored", ctx);

        assertThat(bean.calls).containsExactly(ctx);
        assertThat(adapted.payloadType()).isNull();
    }

    @Test
    void invokesPayloadThenContextInDeclaredOrder() throws Exception {
        Handlers bean = new Handlers();
        MohsJobs.AdaptedHandler adapted = MohsJobs.adaptHandler(bean, method("payloadThenContext", Greeting.class, JobContext.class));
        Greeting payload = new Greeting("ana");
        JobContext ctx = fakeContext();

        adapted.handler().invoke(payload, ctx);

        assertThat(bean.calls).containsExactly(payload, ctx);
        assertThat(adapted.payloadType()).isEqualTo(Greeting.class);
    }

    @Test
    void invokesContextThenPayloadInDeclaredOrder() throws Exception {
        Handlers bean = new Handlers();
        MohsJobs.AdaptedHandler adapted = MohsJobs.adaptHandler(bean, method("contextThenPayload", JobContext.class, Greeting.class));
        Greeting payload = new Greeting("ana");
        JobContext ctx = fakeContext();

        adapted.handler().invoke(payload, ctx);

        assertThat(bean.calls).containsExactly(ctx, payload);
        assertThat(adapted.payloadType()).isEqualTo(Greeting.class);
    }

    @Test
    void rejectsMoreThanTwoParameters() {
        assertThatThrownBy(() -> MohsJobs.adaptHandler(new Handlers(), method("tooManyArgs", Greeting.class, JobContext.class, Greeting.class)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("at most 2 parameters");
    }

    @Test
    void rejectsTwoPayloadParameters() {
        assertThatThrownBy(() -> MohsJobs.adaptHandler(new Handlers(), method("twoPayloads", Greeting.class, Greeting.class)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("more than one non-JobContext");
    }

    @Test
    void rejectsTwoContextParameters() {
        assertThatThrownBy(() -> MohsJobs.adaptHandler(new Handlers(), method("twoContexts", JobContext.class, JobContext.class)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("more than one JobContext");
    }

    @Test
    void unwrapsTheOriginalExceptionFromTheHandler() {
        MohsJobs.AdaptedHandler adapted = MohsJobs.adaptHandler(new Handlers(), method("throwsCheckedFromHandler"));

        assertThatThrownBy(() -> adapted.handler().invoke("x", fakeContext()))
                .isInstanceOf(IOException.class)
                .hasMessage("boom");
    }

    /** A IAE crua do reflection ("argument type mismatch") não diz método nem tipos — o embrulho dá o contexto que {@code Attempt.error()} vai carregar. */
    @Test
    void wrongTypedPayloadFailsNamingMethodAndTypes() {
        MohsJobs.AdaptedHandler adapted = MohsJobs.adaptHandler(new Handlers(), method("payloadOnly", Greeting.class));

        assertThatThrownBy(() -> adapted.handler().invoke("not-a-greeting", fakeContext()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("payloadOnly")
                .hasMessageContaining(Greeting.class.getName())
                .hasMessageContaining(String.class.getName())
                .hasCauseInstanceOf(IllegalArgumentException.class);
    }

    // ---- toDefinition ----

    /** ADR-0037: startPaused viaja da anotação pra definição; ausente, o job nasce armado (default de sempre). */
    @Test
    void translatesStartPaused() {
        JobDefinition dormant = MohsJobs.toDefinition(JobKey.of("dormant-job"), annotationOf("dormantMethod"), AnnotatedFixtures.class);
        JobDefinition armed = MohsJobs.toDefinition(JobKey.of("every-job"), annotationOf("everyMethod"), AnnotatedFixtures.class);

        assertThat(dormant.startPaused()).isTrue();
        assertThat(armed.startPaused()).isFalse();
    }

    @Test
    void translatesCronTrigger() {
        JobDefinition definition = MohsJobs.toDefinition(JobKey.of("cron-job"), annotationOf("cronMethod"), AnnotatedFixtures.class);

        assertThat(definition.schedule()).isEqualTo(new CronSpec("0 0 2 * * *", ZoneId.of("UTC")));
        assertThat(definition.source()).isEqualTo(DefinitionSource.ANNOTATION);
        assertThat(definition.handlerType()).isEqualTo(AnnotatedFixtures.class);
    }

    @Test
    void translatesEveryTrigger() {
        JobDefinition definition = MohsJobs.toDefinition(JobKey.of("every-job"), annotationOf("everyMethod"), AnnotatedFixtures.class);

        assertThat(definition.schedule()).isEqualTo(new IntervalSpec(Duration.ofSeconds(30), false));
    }

    @Test
    void translatesEveryAfterFinishTrigger() {
        JobDefinition definition = MohsJobs.toDefinition(JobKey.of("every-after-finish-job"), annotationOf("everyAfterFinishMethod"), AnnotatedFixtures.class);

        assertThat(definition.schedule()).isEqualTo(new IntervalSpec(Duration.ofMinutes(1), true));
    }

    @Test
    void noTriggerFieldsMeanOnDemand() {
        JobDefinition definition = MohsJobs.toDefinition(JobKey.of("on-demand-job"), annotationOf("onDemandMethod"), AnnotatedFixtures.class);

        assertThat(definition.schedule()).isEqualTo(new OnDemandSpec());
    }

    @Test
    void anAnnotationWithoutRetriesIsBornWithRetryBudget() {
        JobDefinition definition = MohsJobs.toDefinition(JobKey.of("on-demand-job"), annotationOf("onDemandMethod"), AnnotatedFixtures.class);

        assertThat(definition.retries())
                .as("o caminho declarativo tem de nascer com o mesmo orçamento do builder: sem ele o reclaim de "
                        + "uma posse perdida (nó morto, lease vencida) vira FAILED terminal e o contrato "
                        + "at-least-once da ADR-0003 deixa de valer no default")
                .isEqualTo(1);
    }

    @Test
    void rejectsMutuallyExclusiveTriggers() {
        assertThatThrownBy(() -> MohsJobs.toDefinition(JobKey.of("conflicting-job"), annotationOf("conflictingTriggersMethod"), AnnotatedFixtures.class))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("mutually exclusive");
    }

    @Test
    void rejectsCronWithoutZone() {
        assertThatThrownBy(() -> MohsJobs.toDefinition(JobKey.of("cron-no-zone-job"), annotationOf("cronWithoutZoneMethod"), AnnotatedFixtures.class))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("zone");
    }

    @Test
    void blankNameBecomesNull() {
        JobDefinition definition = MohsJobs.toDefinition(JobKey.of("unnamed-job"), annotationOf("unnamedMethod"), AnnotatedFixtures.class);

        assertThat(definition.name()).isNull();
    }

    @Test
    void nonBlankNameIsKept() {
        JobDefinition definition = MohsJobs.toDefinition(JobKey.of("named-job"), annotationOf("namedMethod"), AnnotatedFixtures.class);

        assertThat(definition.name()).isEqualTo("Friendly Name");
    }

    @Test
    void mapsAllowConcurrentExecutionsFalseWithExplicitMax() {
        JobDefinition definition = MohsJobs.toDefinition(JobKey.of("exclusive-job"), annotationOf("exclusiveMethod"), AnnotatedFixtures.class);

        assertThat(definition.allowConcurrentExecutions()).isFalse();
        assertThat(definition.maxConcurrentExecutions()).isEqualTo(3);
    }

    /** maxConcurrentExecutions no default (0) da anotação — o compact constructor de JobDefinition rejeita, mensagem já ensina. */
    @Test
    void allowConcurrentExecutionsFalseWithoutMaxFailsViaJobDefinitionConstructor() {
        assertThatThrownBy(() -> MohsJobs.toDefinition(JobKey.of("invalid-exclusive-job"), annotationOf("invalidExclusiveMethod"), AnnotatedFixtures.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxConcurrentExecutions");
    }

    // ---- diff ----

    @Test
    void diffReportsOnlyChangedFields() {
        JobDefinition original = new JobDefinition(JobKey.of("job"), null, AnnotatedFixtures.class, new OnDemandSpec(),
                null, null, Misfire.IGNORE, true, 0, 0, null, null, DefinitionSource.ANNOTATION);
        JobDefinition changed = new JobDefinition(JobKey.of("job"), null, AnnotatedFixtures.class, new OnDemandSpec(),
                null, null, Misfire.IGNORE, true, 0, 5, null, null, DefinitionSource.ANNOTATION);

        String diff = MohsJobs.diff(original, changed);

        assertThat(diff).contains("retries").doesNotContain("misfire:");
    }
}
