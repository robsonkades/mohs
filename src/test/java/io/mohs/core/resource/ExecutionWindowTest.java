package io.mohs.core.resource;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ExecutionWindowTest {

    @Test
    void excludeWeekendsExcludesSaturdayAndSunday() {
        ExecutionWindow window = ExecutionWindow.named("business-days").excludeWeekends().build();

        Instant saturday = LocalDate.of(2026, 8, 15).atStartOfDay(ZoneOffset.UTC).toInstant(); // sábado
        Instant sunday = LocalDate.of(2026, 8, 16).atStartOfDay(ZoneOffset.UTC).toInstant(); // domingo
        Instant monday = LocalDate.of(2026, 8, 17).atStartOfDay(ZoneOffset.UTC).toInstant(); // segunda

        assertThat(window.excludes(saturday)).isTrue();
        assertThat(window.excludes(sunday)).isTrue();
        assertThat(window.excludes(monday)).isFalse();
    }

    @Test
    void excludeDailyExcludesHalfOpenInterval() {
        ExecutionWindow window = ExecutionWindow.named("maintenance")
                .excludeDaily(LocalTime.MIDNIGHT, LocalTime.of(6, 0))
                .build();

        Instant midnight = LocalDate.of(2026, 8, 17).atTime(0, 0).toInstant(ZoneOffset.UTC);
        Instant fiveFiftyNine = LocalDate.of(2026, 8, 17).atTime(5, 59).toInstant(ZoneOffset.UTC);
        Instant six = LocalDate.of(2026, 8, 17).atTime(6, 0).toInstant(ZoneOffset.UTC);
        Instant noon = LocalDate.of(2026, 8, 17).atTime(12, 0).toInstant(ZoneOffset.UTC);

        assertThat(window.excludes(midnight)).isTrue();
        assertThat(window.excludes(fiveFiftyNine)).isTrue();
        assertThat(window.excludes(six)).isFalse();
        assertThat(window.excludes(noon)).isFalse();
    }

    /** TEST-9: from > to cruza a meia-noite em vez de virar um no-op silencioso. */
    @Test
    void excludeDailyWrapsAroundMidnightWhenFromIsAfterTo() {
        ExecutionWindow window = ExecutionWindow.named("overnight-maintenance")
                .excludeDaily(LocalTime.of(22, 0), LocalTime.of(2, 0))
                .build();

        Instant justBeforeStart = LocalDate.of(2026, 8, 17).atTime(21, 59).toInstant(ZoneOffset.UTC);
        Instant atStart = LocalDate.of(2026, 8, 17).atTime(22, 0).toInstant(ZoneOffset.UTC);
        Instant midnight = LocalDate.of(2026, 8, 18).atTime(0, 0).toInstant(ZoneOffset.UTC);
        Instant justBeforeEnd = LocalDate.of(2026, 8, 18).atTime(1, 59).toInstant(ZoneOffset.UTC);
        Instant atEnd = LocalDate.of(2026, 8, 18).atTime(2, 0).toInstant(ZoneOffset.UTC);

        assertThat(window.excludes(justBeforeStart)).isFalse();
        assertThat(window.excludes(atStart)).isTrue();
        assertThat(window.excludes(midnight)).isTrue();
        assertThat(window.excludes(justBeforeEnd)).isTrue();
        assertThat(window.excludes(atEnd)).isFalse();
    }

    @Test
    void excludeDailyWithEqualFromAndToExcludesNothing() {
        ExecutionWindow window = ExecutionWindow.named("empty")
                .excludeDaily(LocalTime.of(10, 0), LocalTime.of(10, 0))
                .build();

        Instant tenAm = LocalDate.of(2026, 8, 17).atTime(10, 0).toInstant(ZoneOffset.UTC);

        assertThat(window.excludes(tenAm)).isFalse();
    }

    @Test
    void excludeDatesExcludesGivenDates() {
        LocalDate holiday = LocalDate.of(2026, 9, 7);
        ExecutionWindow window = ExecutionWindow.named("holidays").excludeDates(List.of(holiday)).build();

        assertThat(window.excludes(holiday.atStartOfDay(ZoneOffset.UTC).toInstant())).isTrue();
        assertThat(window.excludes(holiday.plusDays(1).atStartOfDay(ZoneOffset.UTC).toInstant())).isFalse();
    }

    @Test
    void combinesMultipleExclusions() {
        ExecutionWindow window = ExecutionWindow.named("combo")
                .excludeWeekends()
                .excludeDaily(LocalTime.MIDNIGHT, LocalTime.of(6, 0))
                .build();

        Instant saturday = LocalDate.of(2026, 8, 15).atTime(12, 0).toInstant(ZoneOffset.UTC);
        Instant weekdayEarlyMorning = LocalDate.of(2026, 8, 17).atTime(3, 0).toInstant(ZoneOffset.UTC);
        Instant weekdayAfternoon = LocalDate.of(2026, 8, 17).atTime(14, 0).toInstant(ZoneOffset.UTC);

        assertThat(window.excludes(saturday)).isTrue();
        assertThat(window.excludes(weekdayEarlyMorning)).isTrue();
        assertThat(window.excludes(weekdayAfternoon)).isFalse();
    }
}
