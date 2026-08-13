package io.mohs.core.definition;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.ZoneId;

import io.mohs.core.job.JobKey;
import io.mohs.core.schedule.CronSpec;
import io.mohs.core.schedule.IntervalSpec;
import io.mohs.core.schedule.Misfire;
import io.mohs.core.schedule.OnDemandSpec;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JobDefinitionTest {

    record Handler() {
    }

    @Test
    void cronBuildsFullyConfiguredDefinition() {
        JobDefinition definition = JobDefinition.of("welcome-email", Handler.class, spec -> spec
                .cron("0 0 2 * * *", ZoneId.of("America/Sao_Paulo"))
                .runner("smtp")
                .queue("emails")
                .window("business-days")
                .misfire(Misfire.FIRE_NOW)
                .retries(8)
                .timeout(Duration.ofMinutes(5))
                .retryPolicy("smtpRetryPolicy"));

        assertThat(definition.key()).isEqualTo(JobKey.of("welcome-email"));
        assertThat(definition.handlerType()).isEqualTo(Handler.class);
        assertThat(definition.schedule()).isEqualTo(new CronSpec("0 0 2 * * *", ZoneId.of("America/Sao_Paulo")));
        assertThat(definition.runner()).isEqualTo("smtp");
        assertThat(definition.queue()).isEqualTo("emails");
        assertThat(definition.window()).isEqualTo("business-days");
        assertThat(definition.misfire()).isEqualTo(Misfire.FIRE_NOW);
        assertThat(definition.retries()).isEqualTo(8);
        assertThat(definition.timeout()).isEqualTo(Duration.ofMinutes(5));
        assertThat(definition.retryPolicy()).isEqualTo("smtpRetryPolicy");
        assertThat(definition.source()).isEqualTo(DefinitionSource.PROGRAMMATIC);
    }

    @Test
    void everyBuildsFixedRateSchedule() {
        JobDefinition definition = JobDefinition.of("poll", Handler.class,
                spec -> spec.every(Duration.ofSeconds(30)).runner("io"));

        assertThat(definition.schedule()).isEqualTo(new IntervalSpec(Duration.ofSeconds(30), false));
    }

    @Test
    void everyAfterFinishBuildsFixedDelaySchedule() {
        JobDefinition definition = JobDefinition.of("drain", Handler.class,
                spec -> spec.everyAfterFinish(Duration.ofMinutes(1)).runner("io"));

        assertThat(definition.schedule()).isEqualTo(new IntervalSpec(Duration.ofMinutes(1), true));
    }

    @Test
    void onDemandBuildsMinimalDefinition() {
        JobDefinition definition = JobDefinition.of("import-file", Handler.class,
                spec -> spec.onDemand().runner("io"));

        assertThat(definition.schedule()).isEqualTo(new OnDemandSpec());
        assertThat(definition.misfire()).isEqualTo(Misfire.IGNORE);
        assertThat(definition.retries()).isZero();
    }

    @Test
    void requiresATriggerToBeChosen() {
        assertThatThrownBy(() -> JobDefinition.of("no-trigger", Handler.class, spec -> {
        })).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void rejectsNegativeRetries() {
        assertThatThrownBy(() -> new JobDefinition(
                JobKey.of("id"), null, Handler.class, new OnDemandSpec(),
                null, null, null, Misfire.IGNORE, false, -1, null, null, DefinitionSource.PROGRAMMATIC))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void allowConcurrentExecutionsDefaultsToFalse() {
        JobDefinition definition = JobDefinition.of("import-file", Handler.class, spec -> spec.onDemand());

        assertThat(definition.allowConcurrentExecutions()).isFalse();
    }

    @Test
    void allowConcurrentExecutionsCanBeOptedIn() {
        JobDefinition definition = JobDefinition.of("import-file", Handler.class,
                spec -> spec.onDemand().allowConcurrentExecutions());

        assertThat(definition.allowConcurrentExecutions()).isTrue();
    }

    @Test
    void rejectsNonPositiveTimeout() {
        assertThatThrownBy(() -> JobDefinition.of("x", Handler.class,
                spec -> spec.onDemand().timeout(Duration.ZERO)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsBlankRunnerQueueWindowRetryPolicy() {
        assertThatThrownBy(() -> JobDefinition.of("x", Handler.class, spec -> spec.onDemand().runner(" ")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> JobDefinition.of("x", Handler.class, spec -> spec.onDemand().queue(" ")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> JobDefinition.of("x", Handler.class, spec -> spec.onDemand().window(" ")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> JobDefinition.of("x", Handler.class, spec -> spec.onDemand().retryPolicy(" ")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsNullKeyHandlerTypeScheduleMisfireSource() {
        assertThatThrownBy(() -> new JobDefinition(
                null, null, Handler.class, new OnDemandSpec(), null, null, null, Misfire.IGNORE, false, 0, null, null, DefinitionSource.PROGRAMMATIC))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new JobDefinition(
                JobKey.of("id"), null, null, new OnDemandSpec(), null, null, null, Misfire.IGNORE, false, 0, null, null, DefinitionSource.PROGRAMMATIC))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new JobDefinition(
                JobKey.of("id"), null, Handler.class, null, null, null, null, Misfire.IGNORE, false, 0, null, null, DefinitionSource.PROGRAMMATIC))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new JobDefinition(
                JobKey.of("id"), null, Handler.class, new OnDemandSpec(), null, null, null, null, false, 0, null, null, DefinitionSource.PROGRAMMATIC))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new JobDefinition(
                JobKey.of("id"), null, Handler.class, new OnDemandSpec(), null, null, null, Misfire.IGNORE, false, 0, null, null, null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void rejectsASecondTriggerChosenInSeparateStatements() {
        assertThatThrownBy(() -> JobDefinition.of("x", Handler.class, spec -> {
            spec.cron("0 0 2 * * *", ZoneId.of("UTC"));
            spec.every(Duration.ofSeconds(30));
        })).isInstanceOf(IllegalStateException.class);
    }
}
