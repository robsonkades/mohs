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
import io.mohs.core.schedule.Schedule;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JobDefinitionTest {

    record Handler() {
    }

    @Test
    void cronBuildsFullyConfiguredDefinition() {
        JobDefinition definition = JobDefinition.of("welcome-email", new io.mohs.core.schedule.CronSpec("0 0 2 * * *", ZoneId.of("America/Sao_Paulo")), io.mohs.core.definition.JobOptions.defaults().withRunner("smtp").withWindow("business-days").withRetries(8).withTimeout(Duration.ofMinutes(5)).withRetryPolicy("smtpRetryPolicy"), Misfire.FIRE_NOW, false);

        assertThat(definition.key()).isEqualTo(JobKey.of("welcome-email"));
        assertThat(definition.executableKey()).isEqualTo(definition.key());
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
        JobDefinition definition = JobDefinition.of("poll", new io.mohs.core.schedule.IntervalSpec(Duration.ofSeconds(30), false), io.mohs.core.definition.JobOptions.defaults().withRunner("io"), io.mohs.core.schedule.Misfire.IGNORE, false);

        assertThat(definition.schedule()).isEqualTo(new IntervalSpec(Duration.ofSeconds(30), false));
    }

    @Test
    void everyAfterFinishBuildsFixedDelaySchedule() {
        JobDefinition definition = JobDefinition.of("drain", new io.mohs.core.schedule.IntervalSpec(Duration.ofMinutes(1), true), io.mohs.core.definition.JobOptions.defaults().withRunner("io"), io.mohs.core.schedule.Misfire.IGNORE, false);

        assertThat(definition.schedule()).isEqualTo(new IntervalSpec(Duration.ofMinutes(1), true));
    }

    @Test
    void onDemandBuildsMinimalDefinition() {
        JobDefinition definition = JobDefinition.of("import-file", new io.mohs.core.schedule.OnDemandSpec(), io.mohs.core.definition.JobOptions.defaults().withRunner("io"), io.mohs.core.schedule.Misfire.IGNORE, false);

        assertThat(definition.schedule()).isEqualTo(new OnDemandSpec());
        assertThat(definition.misfire()).isEqualTo(Misfire.IGNORE);
    }

    @Test
    void aDefinitionWithoutPolicyIsBornWithRetryBudget() {
        JobDefinition definition = JobDefinition.of("import-file", new io.mohs.core.schedule.OnDemandSpec(), io.mohs.core.definition.JobOptions.defaults().withRunner("io"), io.mohs.core.schedule.Misfire.IGNORE, false);

        assertThat(definition.retries())
                .as("a job asking for no policy at all has to be born with budget: without it, lost ownership "
                        + "(a dead node, an expired lease) has nowhere to reschedule and the at-least-once "
                        + "contract stops holding under the defaults")
                .isEqualTo(1);
    }



    @Test
    void rejectsNegativeRetries() {
        assertThatThrownBy(() -> definitionWithPolicy( 0, -1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("retries");
    }

    @Test
    void rejectsNegativeConcurrencyCap() {
        assertThatThrownBy(() -> definitionWithPolicy( -1, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("nonnegative");
    }

    @Test
    void maxConcurrentExecutionsOptsOutOfConcurrentExecutionsWithAnExplicitCap() {
        JobDefinition definition = JobDefinition.of("report-summary", new io.mohs.core.schedule.OnDemandSpec(), io.mohs.core.definition.JobOptions.defaults().withMaxConcurrentExecutions(10), io.mohs.core.schedule.Misfire.IGNORE, false);

        assertThat(definition.maxConcurrentExecutions()).isEqualTo(10);
    }

    @Test
    void preventOverlapCapsAtExactlyOne() {
        JobDefinition definition = JobDefinition.of("import-file", new io.mohs.core.schedule.OnDemandSpec(), io.mohs.core.definition.JobOptions.defaults().withMaxConcurrentExecutions(1), io.mohs.core.schedule.Misfire.IGNORE, false);

        assertThat(definition.maxConcurrentExecutions()).isEqualTo(1);
    }

    @Test
    void concurrencyDefaultsToUnlimited() {
        JobDefinition definition = JobDefinition.of("import-file", new io.mohs.core.schedule.OnDemandSpec(), io.mohs.core.definition.JobOptions.defaults(), io.mohs.core.schedule.Misfire.IGNORE, false);

        assertThat(definition.maxConcurrentExecutions()).isZero();
    }

    @Test
    void preventOverlapOptsOutOfConcurrentExecutions() {
        JobDefinition definition = JobDefinition.of("import-file", new io.mohs.core.schedule.OnDemandSpec(), io.mohs.core.definition.JobOptions.defaults().withMaxConcurrentExecutions(1), io.mohs.core.schedule.Misfire.IGNORE, false);

    }

    @Test
    void rejectsNonPositiveTimeout() {
        assertThatThrownBy(() -> JobDefinition.of("x", new io.mohs.core.schedule.OnDemandSpec(), io.mohs.core.definition.JobOptions.defaults().withTimeout(Duration.ZERO), io.mohs.core.schedule.Misfire.IGNORE, false))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsBlankRunnerWindowRetryPolicy() {
        assertThatThrownBy(() -> JobDefinition.of("x", new io.mohs.core.schedule.OnDemandSpec(), io.mohs.core.definition.JobOptions.defaults().withRunner(" "), io.mohs.core.schedule.Misfire.IGNORE, false))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> JobDefinition.of("x", new io.mohs.core.schedule.OnDemandSpec(), io.mohs.core.definition.JobOptions.defaults().withWindow(" "), io.mohs.core.schedule.Misfire.IGNORE, false))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> JobDefinition.of("x", new io.mohs.core.schedule.OnDemandSpec(), io.mohs.core.definition.JobOptions.defaults().withRetryPolicy(" "), io.mohs.core.schedule.Misfire.IGNORE, false))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsNullKeyHandlerTypeScheduleMisfireSource() {
        assertThatThrownBy(() -> definitionWith(null, JobKey.of("id"), new OnDemandSpec(), Misfire.IGNORE, DefinitionSource.PROGRAMMATIC))
                .isInstanceOf(NullPointerException.class).hasMessage("key");
        assertThatThrownBy(() -> definitionWith(JobKey.of("id"), null, new OnDemandSpec(), Misfire.IGNORE, DefinitionSource.PROGRAMMATIC))
                .isInstanceOf(NullPointerException.class).hasMessage("executableKey");
        assertThatThrownBy(() -> definitionWith(JobKey.of("id"), JobKey.of("id"), null, Misfire.IGNORE, DefinitionSource.PROGRAMMATIC))
                .isInstanceOf(NullPointerException.class).hasMessage("schedule");
        assertThatThrownBy(() -> definitionWith(JobKey.of("id"), JobKey.of("id"), new OnDemandSpec(), null, DefinitionSource.PROGRAMMATIC))
                .isInstanceOf(NullPointerException.class).hasMessage("misfire");
        assertThatThrownBy(() -> definitionWith(JobKey.of("id"), JobKey.of("id"), new OnDemandSpec(), Misfire.IGNORE, null))
                .isInstanceOf(NullPointerException.class).hasMessage("source");
    }



    /**
     * A definition valid in everything but the five components the canonical constructor refuses to
     * accept null for — which this signature spells out, in the order the constructor checks them.
     * Spelled out at the call site instead, the fifteen positional arguments bury the single
     * {@code null} the test is about among the nine that are merely "unset".
     */
    private static JobDefinition definitionWith(
            JobKey key, JobKey executableKey, Schedule schedule, Misfire misfire, DefinitionSource source) {
        return new JobDefinition(key, null, executableKey, schedule,
                null, null, null, misfire, false, 1, 0, null, null, source, "input-v1");
    }

    /**
     * A definition valid in everything but the concurrency ceiling and the retry budget — the trio the
     * canonical constructor cross-validates, and the only thing the rejection tests vary.
     */
    private static JobDefinition definitionWithPolicy( int maxConcurrentExecutions, int retries) {
        return new JobDefinition(JobKey.of("id"), null,JobKey.of("id"), new OnDemandSpec(),
                null, null, null, Misfire.IGNORE, false, maxConcurrentExecutions, retries, null, null, DefinitionSource.PROGRAMMATIC, JobKey.of("id").value());
    }
}
