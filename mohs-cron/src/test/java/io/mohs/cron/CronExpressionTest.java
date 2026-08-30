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
package io.mohs.cron;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies the adaptation (Assert/StringUtils rewritten, the rest copied from Spring) against
 * known cases — including the Quartz L/W/# extensions, since those are exactly the kind of edge an
 * adaptation gets wrong by accident.
 */
class CronExpressionTest {

    @Test
    void everyTenSeconds() {
        CronExpression cron = CronExpression.parse("*/10 * * * * *");
        ZonedDateTime seed = ZonedDateTime.of(2026, 8, 13, 10, 0, 3, 0, ZoneOffset.UTC);

        assertThat(cron.next(seed)).isEqualTo(ZonedDateTime.of(2026, 8, 13, 10, 0, 10, 0, ZoneOffset.UTC));
    }

    @Test
    void dailyAtFixedTimeSameDay() {
        CronExpression cron = CronExpression.parse("0 0 2 * * *");
        ZonedDateTime seed = ZonedDateTime.of(2026, 8, 13, 1, 0, 0, 0, ZoneOffset.UTC);

        assertThat(cron.next(seed)).isEqualTo(ZonedDateTime.of(2026, 8, 13, 2, 0, 0, 0, ZoneOffset.UTC));
    }

    @Test
    void dailyAtFixedTimeRollsToNextDay() {
        CronExpression cron = CronExpression.parse("0 0 2 * * *");
        ZonedDateTime seed = ZonedDateTime.of(2026, 8, 13, 3, 0, 0, 0, ZoneOffset.UTC);

        assertThat(cron.next(seed)).isEqualTo(ZonedDateTime.of(2026, 8, 14, 2, 0, 0, 0, ZoneOffset.UTC));
    }

    @Test
    void nextIsStrictlyAfterAnExactMatch() {
        CronExpression cron = CronExpression.parse("0 0 2 * * *");
        ZonedDateTime exactMatch = ZonedDateTime.of(2026, 8, 13, 2, 0, 0, 0, ZoneOffset.UTC);

        assertThat(cron.next(exactMatch)).isEqualTo(ZonedDateTime.of(2026, 8, 14, 2, 0, 0, 0, ZoneOffset.UTC));
    }

    @Test
    void weekdayRangeSkipsWeekend() {
        CronExpression cron = CronExpression.parse("0 0 9 * * MON-FRI");
        // 2026-08-14 is a Friday; after 9am on Friday, the next one is Monday 2026-08-17
        ZonedDateTime seed = ZonedDateTime.of(2026, 8, 14, 10, 0, 0, 0, ZoneOffset.UTC);

        assertThat(cron.next(seed)).isEqualTo(ZonedDateTime.of(2026, 8, 17, 9, 0, 0, 0, ZoneOffset.UTC));
    }

    @Test
    void dailyMacroMatchesExplicitExpression() {
        CronExpression macro = CronExpression.parse("@daily");
        CronExpression explicit = CronExpression.parse("0 0 0 * * *");
        ZonedDateTime seed = ZonedDateTime.of(2026, 8, 13, 1, 0, 0, 0, ZoneOffset.UTC);

        assertThat(macro.next(seed)).isEqualTo(explicit.next(seed));
        assertThat(macro.next(seed)).isEqualTo(ZonedDateTime.of(2026, 8, 14, 0, 0, 0, 0, ZoneOffset.UTC));
    }

    @Test
    void lastDayOfMonth() {
        CronExpression cron = CronExpression.parse("0 0 0 L * *");
        // August 2026 has 31 days, and the 31st is a Monday
        ZonedDateTime seed = ZonedDateTime.of(2026, 8, 1, 0, 0, 0, 0, ZoneOffset.UTC);

        assertThat(cron.next(seed)).isEqualTo(ZonedDateTime.of(2026, 8, 31, 0, 0, 0, 0, ZoneOffset.UTC));
    }

    @Test
    void thirdToLastDayOfMonth() {
        CronExpression cron = CronExpression.parse("0 0 0 L-3 * *");
        ZonedDateTime seed = ZonedDateTime.of(2026, 8, 1, 0, 0, 0, 0, ZoneOffset.UTC);

        assertThat(cron.next(seed)).isEqualTo(ZonedDateTime.of(2026, 8, 28, 0, 0, 0, 0, ZoneOffset.UTC));
    }

    @Test
    void nearestWeekdayToFirstWhenFirstIsSaturdayRollsForwardWithinMonth() {
        // 2026-08-01 is a Saturday; "1W" must resolve to Monday 2026-08-03, not to
        // Friday 2026-07-31 (the documented special rule for day 1).
        CronExpression cron = CronExpression.parse("0 0 0 1W * *");
        ZonedDateTime seed = ZonedDateTime.of(2026, 7, 25, 0, 0, 0, 0, ZoneOffset.UTC);

        assertThat(cron.next(seed)).isEqualTo(ZonedDateTime.of(2026, 8, 3, 0, 0, 0, 0, ZoneOffset.UTC));
    }

    @Test
    void nearestWeekdayToFirstWhenFirstIsSundayRollsForward() {
        // 2026-11-01 is a Sunday; "1W" must resolve to Monday 2026-11-02.
        CronExpression cron = CronExpression.parse("0 0 0 1W * *");
        ZonedDateTime seed = ZonedDateTime.of(2026, 10, 25, 0, 0, 0, 0, ZoneOffset.UTC);

        assertThat(cron.next(seed)).isEqualTo(ZonedDateTime.of(2026, 11, 2, 0, 0, 0, 0, ZoneOffset.UTC));
    }

    @Test
    void lastWeekdayOfMonth() {
        // 2026-08-31 (a Monday) is already a weekday, so LW lands on it.
        CronExpression cron = CronExpression.parse("0 0 0 LW * *");
        ZonedDateTime seed = ZonedDateTime.of(2026, 8, 1, 0, 0, 0, 0, ZoneOffset.UTC);

        assertThat(cron.next(seed)).isEqualTo(ZonedDateTime.of(2026, 8, 31, 0, 0, 0, 0, ZoneOffset.UTC));
    }

    @Test
    void lastFridayOfMonth() {
        // The last Friday of August 2026 is the 28th.
        CronExpression cron = CronExpression.parse("0 0 0 * * 5L");
        ZonedDateTime seed = ZonedDateTime.of(2026, 8, 1, 0, 0, 0, 0, ZoneOffset.UTC);

        assertThat(cron.next(seed)).isEqualTo(ZonedDateTime.of(2026, 8, 28, 0, 0, 0, 0, ZoneOffset.UTC));
    }

    @Test
    void secondFridayOfMonth() {
        // The second Friday of September 2026 is the 11th (the first is the 4th).
        CronExpression cron = CronExpression.parse("0 0 0 ? * 5#2");
        ZonedDateTime seed = ZonedDateTime.of(2026, 9, 1, 0, 0, 0, 0, ZoneOffset.UTC);

        assertThat(cron.next(seed)).isEqualTo(ZonedDateTime.of(2026, 9, 11, 0, 0, 0, 0, ZoneOffset.UTC));
    }

    @Test
    void preservesTheZoneOfTheSeed() {
        CronExpression cron = CronExpression.parse("0 30 14 * * *");
        ZonedDateTime seed = ZonedDateTime.of(2026, 8, 13, 10, 0, 0, 0, java.time.ZoneId.of("America/Sao_Paulo"));

        ZonedDateTime next = cron.next(seed);

        assertThat(next.getZone()).isEqualTo(java.time.ZoneId.of("America/Sao_Paulo"));
        assertThat(next).isEqualTo(ZonedDateTime.of(2026, 8, 13, 14, 30, 0, 0, java.time.ZoneId.of("America/Sao_Paulo")));
    }

    @Test
    void rejectsWrongFieldCount() {
        assertThatThrownBy(() -> CronExpression.parse("0 0"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("6 fields");
    }

    @Test
    void rejectsGarbage() {
        assertThatThrownBy(() -> CronExpression.parse("not a cron expression at all"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void isValidExpressionReportsCorrectly() {
        assertThat(CronExpression.isValidExpression("0 0 2 * * *")).isTrue();
        assertThat(CronExpression.isValidExpression("garbage")).isFalse();
        assertThat(CronExpression.isValidExpression(null)).isFalse();
    }

    /**
     * The regression guard for strict progress. {@code next()} promises an instant strictly after
     * the seed, and its consumer ({@code FiringPlanner.planSeries}) ITERATES over the result — so a
     * fixed point becomes 1,440 materialisations of the same occurrence per tick, forever. The
     * defect only surfaced in day-of-month {@code L-n} ({@code L-28} stuck on the 3rd step,
     * {@code L-30} on the 2nd), so this list covers the whole family of Quartz extensions and
     * iterates deep enough to cross month ends, year ends and a leap February.
     */
    @ParameterizedTest
    @ValueSource(strings = {"*/10 * * * * *", "0 0 2 * * *", "0 0 0 L * *", "0 0 0 L-3 * *",
            "0 0 0 L-28 * *", "0 0 0 L-30 * *", "0 0 0 LW * *", "0 0 0 1W * *",
            "0 0 0 * * 5L", "0 0 0 ? * 5#2", "0 0 0 29 2 *"})
    void nextIsAlwaysStrictlyAfterTheSeed(String expression) {
        CronExpression cron = CronExpression.parse(expression);
        ZonedDateTime seed = ZonedDateTime.of(2026, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);

        for (int i = 0; i < 200; i++) {
            ZonedDateTime next = cron.next(seed);
            assertThat(next).as("iteration %d of '%s' from %s", i, expression, seed)
                    .isNotNull()
                    .isAfter(seed);
            seed = next;
        }
    }
}
