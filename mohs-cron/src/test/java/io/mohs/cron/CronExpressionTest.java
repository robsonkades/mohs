package io.mohs.cron;

import org.junit.jupiter.api.Test;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifica a adaptação (Assert/StringUtils reescritos, resto copiado do
 * Spring) contra casos conhecidos — incluindo as extensões Quartz L/W/#,
 * já que são exatamente o tipo de borda onde uma adaptação erra por
 * descuido.
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
        // 2026-08-14 é sexta-feira; após as 9h de sexta, o próximo é segunda 2026-08-17
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
        // agosto/2026 tem 31 dias, e o dia 31 é uma segunda-feira
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
        // 2026-08-01 é sábado; "1W" deve resolver para segunda 2026-08-03,
        // não para sexta 2026-07-31 (regra especial documentada para dia 1).
        CronExpression cron = CronExpression.parse("0 0 0 1W * *");
        ZonedDateTime seed = ZonedDateTime.of(2026, 7, 25, 0, 0, 0, 0, ZoneOffset.UTC);

        assertThat(cron.next(seed)).isEqualTo(ZonedDateTime.of(2026, 8, 3, 0, 0, 0, 0, ZoneOffset.UTC));
    }

    @Test
    void nearestWeekdayToFirstWhenFirstIsSundayRollsForward() {
        // 2026-11-01 é domingo; "1W" deve resolver para segunda 2026-11-02.
        CronExpression cron = CronExpression.parse("0 0 0 1W * *");
        ZonedDateTime seed = ZonedDateTime.of(2026, 10, 25, 0, 0, 0, 0, ZoneOffset.UTC);

        assertThat(cron.next(seed)).isEqualTo(ZonedDateTime.of(2026, 11, 2, 0, 0, 0, 0, ZoneOffset.UTC));
    }

    @Test
    void lastWeekdayOfMonth() {
        // 2026-08-31 (segunda-feira) já é dia útil, então LW cai nele mesmo.
        CronExpression cron = CronExpression.parse("0 0 0 LW * *");
        ZonedDateTime seed = ZonedDateTime.of(2026, 8, 1, 0, 0, 0, 0, ZoneOffset.UTC);

        assertThat(cron.next(seed)).isEqualTo(ZonedDateTime.of(2026, 8, 31, 0, 0, 0, 0, ZoneOffset.UTC));
    }

    @Test
    void lastFridayOfMonth() {
        // última sexta-feira de agosto/2026 é dia 28.
        CronExpression cron = CronExpression.parse("0 0 0 * * 5L");
        ZonedDateTime seed = ZonedDateTime.of(2026, 8, 1, 0, 0, 0, 0, ZoneOffset.UTC);

        assertThat(cron.next(seed)).isEqualTo(ZonedDateTime.of(2026, 8, 28, 0, 0, 0, 0, ZoneOffset.UTC));
    }

    @Test
    void secondFridayOfMonth() {
        // segunda sexta-feira de setembro/2026 é dia 11 (a primeira é dia 4).
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
}
