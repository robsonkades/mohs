package io.mohs.jdbc;

import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;

import io.mohs.engine.SyncableClock;

/**
 * Implementação "database" das três de
 * {@code docs/adr/0008-configurable-time-source.md}: o banco é a
 * autoridade de tempo do cluster — {@link #instant()} nunca faz I/O, é
 * O(1) sobre o offset já amostrado por {@link #sync()}. Implementa
 * {@link SyncableClock} (porta em {@code io.mohs.engine}) em vez de expor
 * {@code sync()}/{@code currentOffset()} só como métodos concretos —
 * mesmo padrão adapter de {@code JdbcJobStore}/{@code JobStore}.
 *
 * <p>Só o offset é responsabilidade desta classe — "de quanto em quanto
 * tempo reamostrar" é decisão de quem a usa (agendamento entra em
 * {@code io.mohs.autoconfigure}, junto do resto do property binding de
 * {@code mohs.time.*}).
 *
 * <p>É o único lugar do motor onde ler o relógio de verdade
 * ({@link Clock#systemUTC()}) é o propósito da classe, não uma violação
 * da regra "todo agora vem do Clock injetado" — {@code ArchitectureTest}
 * abre exceção só para esta classe.
 */
public final class DatabaseClock extends Clock implements SyncableClock {

    private static final Logger log = LoggerFactory.getLogger(DatabaseClock.class);
    private static final String NOW_QUERY = "SELECT CURRENT_TIMESTAMP";

    private final JdbcTemplate jdbcTemplate;
    private final Duration skewWarnThreshold;
    private final ZoneId zone;
    private final Clock systemClock;
    private final AtomicReference<Duration> offset = new AtomicReference<>(Duration.ZERO);

    public DatabaseClock(DataSource dataSource, Duration skewWarnThreshold) {
        this(dataSource, skewWarnThreshold, ZoneId.of("UTC"), Clock.systemUTC());
    }

    DatabaseClock(DataSource dataSource, Duration skewWarnThreshold, ZoneId zone, Clock systemClock) {
        this.jdbcTemplate = new JdbcTemplate(Objects.requireNonNull(dataSource, "dataSource"));
        this.skewWarnThreshold = Objects.requireNonNull(skewWarnThreshold, "skewWarnThreshold");
        this.zone = Objects.requireNonNull(zone, "zone");
        this.systemClock = Objects.requireNonNull(systemClock, "systemClock");
    }

    @Override
    public ZoneId getZone() {
        return zone;
    }

    /** View com outro zone, delegando {@link #instant()} pra este mesmo relógio. */
    @Override
    public Clock withZone(ZoneId zone) {
        Objects.requireNonNull(zone, "zone");
        return new Clock() {
            @Override
            public ZoneId getZone() {
                return zone;
            }

            @Override
            public Clock withZone(ZoneId newZone) {
                return DatabaseClock.this.withZone(newZone);
            }

            @Override
            public Instant instant() {
                return DatabaseClock.this.instant();
            }
        };
    }

    @Override
    public Instant instant() {
        return systemClock.instant().plus(offset.get());
    }

    @Override
    public Duration currentOffset() {
        return offset.get();
    }

    /** Mede o offset banco×app com compensação de ida-e-volta e aplica o clamp monotônico. */
    @Override
    public void sync() {
        try {
            Instant beforeQuery = systemClock.instant();
            long t0 = System.nanoTime();
            Timestamp databaseTimestamp = jdbcTemplate.queryForObject(NOW_QUERY, Timestamp.class);
            long t1 = System.nanoTime();

            if (databaseTimestamp == null) {
                log.warn("'{}' returned no result, keeping last known offset {}", NOW_QUERY, offset.get());
                return;
            }

            Duration roundTrip = Duration.ofNanos(t1 - t0);
            Instant appNowAtMidpoint = beforeQuery.plus(roundTrip.dividedBy(2));
            Duration sampledOffset = Duration.between(appNowAtMidpoint, databaseTimestamp.toInstant());

            if (sampledOffset.abs().compareTo(skewWarnThreshold) > 0) {
                log.warn("clock skew {} exceeds threshold {}", sampledOffset, skewWarnThreshold);
            }

            applyIfMonotonic(sampledOffset);
        } catch (DataAccessException e) {
            log.warn("failed to sync clock with database, keeping last known offset {}", offset.get(), e);
        }
    }

    /**
     * "now + sampledOffset < now + offset atual" simplifica pra
     * "sampledOffset < offset atual" — o {@code now} é o mesmo dos dois
     * lados, não precisa ler o relógio de novo pra comparar. Reamostragem
     * que voltaria no tempo (ADR-0008) é descartada — não ajustada pra um
     * valor mínimo seguro — e tenta de novo na próxima chamada, quando o
     * tempo real já terá avançado o bastante.
     *
     * <p>{@code accumulateAndGet} torna o clamp atômico independente de
     * quem chama {@link #sync()} — nada assume escritor único; se duas
     * amostragens concorrentes chegam aqui, a comparação e a escrita
     * acontecem como uma unidade só, sem janela para uma escrita mais
     * antiga sobrescrever uma mais nova já publicada.
     */
    private void applyIfMonotonic(Duration sampledOffset) {
        offset.accumulateAndGet(sampledOffset, (current, sampled) -> sampled.compareTo(current) < 0 ? current : sampled);
    }
}
