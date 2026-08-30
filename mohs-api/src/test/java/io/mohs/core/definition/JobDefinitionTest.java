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
                .window("business-days")
                .misfire(Misfire.FIRE_NOW)
                .retries(8)
                .timeout(Duration.ofMinutes(5))
                .retryPolicy("smtpRetryPolicy"));

        assertThat(definition.key()).isEqualTo(JobKey.of("welcome-email"));
        assertThat(definition.handlerType()).isEqualTo(Handler.class);
        assertThat(definition.schedule()).isEqualTo(new CronSpec("0 0 2 * * *", ZoneId.of("America/Sao_Paulo")));
        assertThat(definition.runner()).isEqualTo("smtp");
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
    }

    @Test
    void aDefinitionWithoutPolicyIsBornWithRetryBudget() {
        JobDefinition definition = JobDefinition.of("import-file", Handler.class,
                spec -> spec.onDemand().runner("io"));

        assertThat(definition.retries())
                .as("a job asking for no policy at all has to be born with budget: without it, lost ownership "
                        + "(a dead node, an expired lease) has nowhere to reschedule and the at-least-once "
                        + "contract stops holding under the defaults")
                .isEqualTo(1);
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
                null, null, Misfire.IGNORE, true, 0, -1, null, null, DefinitionSource.PROGRAMMATIC))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsMaxConcurrentExecutionsSetWhenConcurrencyIsAllowed() {
        assertThatThrownBy(() -> new JobDefinition(
                JobKey.of("id"), null, Handler.class, new OnDemandSpec(),
                null, null, Misfire.IGNORE, true, 1, 0, null, null, DefinitionSource.PROGRAMMATIC))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsNonPositiveMaxConcurrentExecutionsWhenConcurrencyIsNotAllowed() {
        assertThatThrownBy(() -> new JobDefinition(
                JobKey.of("id"), null, Handler.class, new OnDemandSpec(),
                null, null, Misfire.IGNORE, false, 0, 0, null, null, DefinitionSource.PROGRAMMATIC))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void maxConcurrentExecutionsOptsOutOfConcurrentExecutionsWithAnExplicitCap() {
        JobDefinition definition = JobDefinition.of("report-summary", Handler.class,
                spec -> spec.onDemand().maxConcurrentExecutions(10));

        assertThat(definition.allowConcurrentExecutions()).isFalse();
        assertThat(definition.maxConcurrentExecutions()).isEqualTo(10);
    }

    @Test
    void preventOverlapCapsAtExactlyOne() {
        JobDefinition definition = JobDefinition.of("import-file", Handler.class,
                spec -> spec.onDemand().preventOverlap());

        assertThat(definition.maxConcurrentExecutions()).isEqualTo(1);
    }

    @Test
    void allowConcurrentExecutionsDefaultsToTrue() {
        JobDefinition definition = JobDefinition.of("import-file", Handler.class, spec -> spec.onDemand());

        assertThat(definition.allowConcurrentExecutions()).isTrue();
    }

    @Test
    void preventOverlapOptsOutOfConcurrentExecutions() {
        JobDefinition definition = JobDefinition.of("import-file", Handler.class,
                spec -> spec.onDemand().preventOverlap());

        assertThat(definition.allowConcurrentExecutions()).isFalse();
    }

    @Test
    void rejectsNonPositiveTimeout() {
        assertThatThrownBy(() -> JobDefinition.of("x", Handler.class,
                spec -> spec.onDemand().timeout(Duration.ZERO)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsBlankRunnerWindowRetryPolicy() {
        assertThatThrownBy(() -> JobDefinition.of("x", Handler.class, spec -> spec.onDemand().runner(" ")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> JobDefinition.of("x", Handler.class, spec -> spec.onDemand().window(" ")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> JobDefinition.of("x", Handler.class, spec -> spec.onDemand().retryPolicy(" ")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsNullKeyHandlerTypeScheduleMisfireSource() {
        assertThatThrownBy(() -> new JobDefinition(
                null, null, Handler.class, new OnDemandSpec(), null, null, Misfire.IGNORE, false, 1, 0, null, null, DefinitionSource.PROGRAMMATIC))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new JobDefinition(
                JobKey.of("id"), null, null, new OnDemandSpec(), null, null, Misfire.IGNORE, false, 1, 0, null, null, DefinitionSource.PROGRAMMATIC))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new JobDefinition(
                JobKey.of("id"), null, Handler.class, null, null, null, Misfire.IGNORE, false, 1, 0, null, null, DefinitionSource.PROGRAMMATIC))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new JobDefinition(
                JobKey.of("id"), null, Handler.class, new OnDemandSpec(), null, null, null, false, 1, 0, null, null, DefinitionSource.PROGRAMMATIC))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new JobDefinition(
                JobKey.of("id"), null, Handler.class, new OnDemandSpec(), null, null, Misfire.IGNORE, false, 1, 0, null, null, null))
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
