package io.mohs.engine;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Optional;

import io.mohs.schedule.CronSpec;
import io.mohs.schedule.IntervalSpec;
import io.mohs.schedule.OnDemandSpec;

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
        // referência em UTC, mas o CronSpec é em America/Sao_Paulo (UTC-3)
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
        // 30 de fevereiro não existe em nenhum ano
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
        // a distinção fixed-rate x fixed-delay está em o QUE o chamador passa
        // como reference (agendado x fim da execução) — não em lógica daqui.
        IntervalSpec fixedDelay = new IntervalSpec(Duration.ofMinutes(30), true);
        Instant executionEndedAt = Instant.parse("2026-08-13T10:05:00Z");

        Optional<Instant> next = calculator.nextFireAfter(fixedDelay, executionEndedAt);

        assertThat(next).contains(Instant.parse("2026-08-13T10:35:00Z"));
    }

    @Test
    void onDemandNeverFiresAutomatically() {
        Optional<Instant> next = calculator.nextFireAfter(new OnDemandSpec(), Instant.now());

        assertThat(next).isEmpty();
    }
}
