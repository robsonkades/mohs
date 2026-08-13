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
                .timeout(Duration.ofMinutes(5)));

        assertThat(definition.key()).isEqualTo(JobKey.of("welcome-email"));
        assertThat(definition.handlerType()).isEqualTo(Handler.class);
        assertThat(definition.schedule()).isEqualTo(new CronSpec("0 0 2 * * *", ZoneId.of("America/Sao_Paulo")));
        assertThat(definition.runner()).isEqualTo("smtp");
        assertThat(definition.queue()).isEqualTo("emails");
        assertThat(definition.window()).isEqualTo("business-days");
        assertThat(definition.misfire()).isEqualTo(Misfire.FIRE_NOW);
        assertThat(definition.retries()).isEqualTo(8);
        assertThat(definition.timeout()).isEqualTo(Duration.ofMinutes(5));
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
                null, null, null, Misfire.IGNORE, -1, null, DefinitionSource.PROGRAMMATIC))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
