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
package io.mohs.engine;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Optional;

import io.mohs.core.schedule.CronSpec;
import io.mohs.core.schedule.IntervalSpec;
import io.mohs.core.schedule.OnDemandSpec;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class NextFireCalculatorTest {

    private final NextFireCalculator calculator = new NextFireCalculator();

    @Test
    void cronDelegatesToCronExpression() {
        CronSpec cron = new CronSpec("0 0 2 * * *", ZoneId.of("UTC"));
        Instant reference = ZonedDateTime.of(2026, 8, 13, 1, 0, 0, 0, ZoneId.of("UTC")).toInstant();

        Optional<Instant> next = calculator.nextFireAfter(cron, reference);

        assertThat(next).contains(ZonedDateTime.of(2026, 8, 13, 2, 0, 0, 0, ZoneId.of("UTC")).toInstant());
    }

    @Test
    void cronRespectsItsOwnZoneRegardlessOfReferenceZone() {
        // The reference is in UTC, but the CronSpec is in America/Sao_Paulo (UTC-3)
        CronSpec cron = new CronSpec("0 30 14 * * *", ZoneId.of("America/Sao_Paulo"));
        Instant reference = ZonedDateTime.of(2026, 8, 13, 10, 0, 0, 0, ZoneId.of("UTC")).toInstant();

        Optional<Instant> next = calculator.nextFireAfter(cron, reference);

        // 14:30 em America/Sao_Paulo (UTC-3) = 17:30 UTC
        assertThat(next).contains(ZonedDateTime.of(2026, 8, 13, 17, 30, 0, 0, ZoneId.of("UTC")).toInstant());
    }

    @Test
    void repeatedCallsWithTheSameExpressionReuseTheParsedCache() {
        CronSpec cron = new CronSpec("0 0 2 * * *", ZoneId.of("UTC"));
        Instant first = ZonedDateTime.of(2026, 8, 13, 1, 0, 0, 0, ZoneId.of("UTC")).toInstant();
        Instant second = ZonedDateTime.of(2026, 8, 14, 3, 0, 0, 0, ZoneId.of("UTC")).toInstant();

        assertThat(calculator.nextFireAfter(cron, first))
                .contains(ZonedDateTime.of(2026, 8, 13, 2, 0, 0, 0, ZoneId.of("UTC")).toInstant());
        assertThat(calculator.nextFireAfter(cron, second))
                .contains(ZonedDateTime.of(2026, 8, 15, 2, 0, 0, 0, ZoneId.of("UTC")).toInstant());
    }

    @Test
    void rejectsCronExpressionThatNeverFires() {
        // 30 February does not exist in any year
        CronSpec impossible = new CronSpec("0 0 0 30 2 *", ZoneId.of("UTC"));
        Instant reference = Instant.parse("2026-01-01T00:00:00Z");

        assertThatThrownBy(() -> calculator.nextFireAfter(impossible, reference))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("never fires");
    }

    @Test
    void fixedRateAddsIntervalToReference() {
        IntervalSpec fixedRate = new IntervalSpec(Duration.ofMinutes(30), false);
        Instant reference = Instant.parse("2026-08-13T10:00:00Z");

        Optional<Instant> next = calculator.nextFireAfter(fixedRate, reference);

        assertThat(next).contains(Instant.parse("2026-08-13T10:30:00Z"));
    }

    @Test
    void fixedDelayAlsoJustAddsIntervalToWhateverReferenceItIsGiven() {
        // The fixed-rate versus fixed-delay distinction lies in WHAT the caller passes as reference
        // (scheduled time versus end of execution) — not in any logic here.
        IntervalSpec fixedDelay = new IntervalSpec(Duration.ofMinutes(30), true);
        Instant executionEndedAt = Instant.parse("2026-08-13T10:05:00Z");

        Optional<Instant> next = calculator.nextFireAfter(fixedDelay, executionEndedAt);

        assertThat(next).contains(Instant.parse("2026-08-13T10:35:00Z"));
    }

    @Test
    void onDemandNeverFiresAutomatically() {
        Optional<Instant> next = calculator.nextFireAfter(new OnDemandSpec(), Instant.EPOCH);

        assertThat(next).isEmpty();
    }

    /**
     * On 2026-10-25 the time 02:00 happens twice in Berlin (+02:00 and then +01:00) and the cron matches
     * both — a daily close executed TWICE that day. The second is suppressed: a loss is worse than a
     * delay.
     */
    @Test
    void dstFallBackFiresTheRepeatedWallClockTimeOnlyOnce() {
        ZoneId berlin = ZoneId.of("Europe/Berlin");
        CronSpec daily = new CronSpec("0 0 2 * * *", berlin);
        Instant firstFire = ZonedDateTime.of(2026, 10, 25, 2, 0, 0, 0, berlin).toInstant();

        assertThat(calculator.nextFireAfter(daily, firstFire))
                .as("the 02:00 repeat at offset +01:00 must not become a second execution")
                .contains(ZonedDateTime.of(2026, 10, 26, 2, 0, 0, 0, berlin).toInstant());
    }

    /**
     * The other side of that decision: the deduplication applies to a REPEATED LABEL, not to every
     * occurrence in the ambiguous window. On an hourly cron, Berlin's two 02:00s are real instants an
     * hour apart — suppressing the second would leave TWO hours of real time with a single firing, once
     * a year, and a job reconciling "the last hour" would lose an hour of data with nothing in the log.
     */
    @Test
    void dstFallBackKeepsBothHoursOfAnHourlyCron() {
        ZoneId berlin = ZoneId.of("Europe/Berlin");
        CronSpec hourly = new CronSpec("0 0 * * * *", berlin);
        Instant firstTwoAm = ZonedDateTime.of(2026, 10, 25, 2, 0, 0, 0, berlin).toInstant();

        assertThat(calculator.nextFireAfter(hourly, firstTwoAm))
                .as("the second 02:00 (offset +01:00) is one more hour of work, not a duplicate")
                .contains(firstTwoAm.plus(Duration.ofHours(1)));
    }

    /**
     * The deduplication's criterion is the schedule's STEP against the fall-back's shift, not "there is
     * another occurrence today". A sparse twice-daily cron passes that calendar proxy and used to
     * duplicate again: Berlin's two 02:00s on 25 October are the SAME close running twice, not new work
     * — the real next one is 23:00.
     */
    @Test
    void dstFallBackDoesNotDuplicateASparseTwiceDailyCron() {
        ZoneId berlin = ZoneId.of("Europe/Berlin");
        CronSpec twiceDaily = new CronSpec("0 0 2,23 * * *", berlin);
        Instant firstTwoAm = ZonedDateTime.of(2026, 10, 25, 2, 0, 0, 0, berlin).toInstant();

        assertThat(calculator.nextFireAfter(twiceDaily, firstTwoAm))
                .contains(ZonedDateTime.of(2026, 10, 25, 23, 0, 0, 0, berlin).toInstant());
    }

    /**
     * The criterion cannot look only FORWARD. A cron of ADJACENT hours ("run the close at 02:00 and the
     * report at 03:00") has a 1h step after 02:00 and NO occurrence before it — the one-directional
     * predicate read it as dense and duplicated the close. Measured: 1,346 duplicates in a sweep of the
     * entire TZDB, all of this shape.
     */
    @Test
    void dstFallBackDoesNotDuplicateACronOfAdjacentHours() {
        ZoneId berlin = ZoneId.of("Europe/Berlin");
        CronSpec adjacentHours = new CronSpec("0 0 2,3 * * *", berlin);
        Instant firstTwoAm = ZonedDateTime.of(2026, 10, 25, 2, 0, 0, 0, berlin).toInstant();

        assertThat(calculator.nextFireAfter(adjacentHours, firstTwoAm))
                .as("the series is not uniformly dense: there is no occurrence BEFORE 02:00")
                .contains(ZonedDateTime.of(2026, 10, 25, 3, 0, 0, 0, berlin).toInstant());
    }

    /**
     * On 2026-03-29 the time 02:00 DOES NOT EXIST in Berlin. The firing is not recovered at 03:00 (a
     * deliberate divergence from Quartz): the next occurrence is the 30th's.
     */
    @Test
    void dstSpringForwardSkipsTheDayWhoseWallClockTimeDoesNotExist() {
        ZoneId berlin = ZoneId.of("Europe/Berlin");
        CronSpec daily = new CronSpec("0 0 2 * * *", berlin);
        Instant beforeTheGap = ZonedDateTime.of(2026, 3, 28, 2, 0, 0, 0, berlin).toInstant();

        assertThat(calculator.nextFireAfter(daily, beforeTheGap))
                .contains(ZonedDateTime.of(2026, 3, 30, 2, 0, 0, 0, berlin).toInstant());
    }
}
