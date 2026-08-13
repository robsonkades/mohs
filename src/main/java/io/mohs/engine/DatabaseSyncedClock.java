package io.mohs.engine;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import javax.sql.DataSource;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementação "database" das três de
 * {@code docs/adr/0008-configurable-time-source.md}: o banco é a
 * autoridade de tempo do cluster, amostrada em background estilo NTP —
 * {@link #instant()} nunca faz I/O, é O(1) sobre o offset já amostrado.
 *
 * <p>É o único lugar do motor onde ler o relógio de verdade
 * ({@link Clock#systemUTC()}) é o propósito da classe, não uma violação
 * da regra "todo agora vem do Clock injetado" — {@code ArchitectureTest}
 * abre exceção só para esta classe.
 */
public final class DatabaseSyncedClock extends Clock implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(DatabaseSyncedClock.class);
    private static final String NOW_QUERY = "SELECT CURRENT_TIMESTAMP";

    private final DataSource dataSource;
    private final Duration syncInterval;
    private final Duration skewWarnThreshold;
    private final ZoneId zone;
    private final Clock systemClock;
    private final AtomicReference<Duration> offset;
    private final AtomicReference<Instant> lastReturnedInstant;

    private volatile boolean running;
    private @Nullable Thread samplerThread;

    public DatabaseSyncedClock(DataSource dataSource, Duration syncInterval, Duration skewWarnThreshold) {
        this(dataSource, syncInterval, skewWarnThreshold, ZoneId.of("UTC"), Clock.systemUTC());
    }

    DatabaseSyncedClock(DataSource dataSource, Duration syncInterval, Duration skewWarnThreshold,
            ZoneId zone, Clock systemClock) {
        this(dataSource, syncInterval, skewWarnThreshold, zone, systemClock,
                new AtomicReference<>(Duration.ZERO), new AtomicReference<>(systemClock.instant()));
    }

    /**
     * Construtor de view (ver {@link #withZone}) — compartilha o offset e
     * o relógio monotônico já amostrados em vez de congelar um snapshot.
     */
    private DatabaseSyncedClock(DataSource dataSource, Duration syncInterval, Duration skewWarnThreshold,
            ZoneId zone, Clock systemClock, AtomicReference<Duration> offset, AtomicReference<Instant> lastReturnedInstant) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
        this.syncInterval = Objects.requireNonNull(syncInterval, "syncInterval");
        this.skewWarnThreshold = Objects.requireNonNull(skewWarnThreshold, "skewWarnThreshold");
        this.zone = Objects.requireNonNull(zone, "zone");
        this.systemClock = Objects.requireNonNull(systemClock, "systemClock");
        this.offset = offset;
        this.lastReturnedInstant = lastReturnedInstant;
    }

    /** Sobe a virtual thread de amostragem em background. Idempotente. */
    public void start() {
        if (running) {
            return;
        }
        running = true;
        samplerThread = Thread.ofVirtual().name("mohs-clock-sync").start(this::samplingLoop);
    }

    @Override
    public void close() {
        running = false;
        Thread thread = samplerThread;
        if (thread != null) {
            thread.interrupt();
        }
    }

    @Override
    public ZoneId getZone() {
        return zone;
    }

    /**
     * View com outro zone, compartilhando o offset e o relógio monotônico
     * já amostrados — não sobe uma segunda virtual thread de amostragem;
     * {@link #close()} nesta view não afeta a original.
     */
    @Override
    public Clock withZone(ZoneId zone) {
        Objects.requireNonNull(zone, "zone");
        return new DatabaseSyncedClock(dataSource, syncInterval, skewWarnThreshold, zone, systemClock, offset, lastReturnedInstant);
    }

    @Override
    public Instant instant() {
        Instant candidate = systemClock.instant().plus(offset.get());
        return lastReturnedInstant.updateAndGet(previous -> candidate.isAfter(previous) ? candidate : previous);
    }

    /** Offset atual (banco − app), exposto pra quando a infra de métricas existir. */
    public Duration currentOffset() {
        return offset.get();
    }

    private void samplingLoop() {
        while (running) {
            sampleOnce();
            try {
                Thread.sleep(syncInterval);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    /** Package-private: uma amostra isolada, sem esperar o loop — usado direto pelos testes. */
    void sampleOnce() {
        try (Connection connection = dataSource.getConnection()) {
            Instant beforeQuery = systemClock.instant();
            long t0 = System.nanoTime();
            Instant databaseNow = queryDatabaseNow(connection);
            long t1 = System.nanoTime();
            Duration roundTrip = Duration.ofNanos(t1 - t0);

            Instant appNowAtMidpoint = beforeQuery.plus(roundTrip.dividedBy(2));
            Duration sampledOffset = Duration.between(appNowAtMidpoint, databaseNow);

            if (sampledOffset.abs().compareTo(skewWarnThreshold) > 0) {
                log.warn("clock skew {} exceeds threshold {}", sampledOffset, skewWarnThreshold);
            }

            applyClamped(sampledOffset);
        } catch (SQLException e) {
            log.warn("failed to sync clock with database, keeping last known offset {}", offset.get(), e);
        }
    }

    private void applyClamped(Duration sampledOffset) {
        Instant previous = lastReturnedInstant.get();
        Instant candidateInstant = systemClock.instant().plus(sampledOffset);
        if (!candidateInstant.isAfter(previous)) {
            // reamostragem não anda pra trás (ADR-0008): mantém o offset mínimo que
            // preserva instant() estritamente crescente em vez do offset amostrado cru.
            offset.set(Duration.between(systemClock.instant(), previous.plusNanos(1)));
            return;
        }
        offset.set(sampledOffset);
    }

    private static Instant queryDatabaseNow(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(NOW_QUERY)) {
            resultSet.next();
            return resultSet.getTimestamp(1).toInstant();
        }
    }
}
