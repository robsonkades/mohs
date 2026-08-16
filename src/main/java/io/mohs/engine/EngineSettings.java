package io.mohs.engine;

import java.time.Duration;
import java.util.Objects;

import org.jspecify.annotations.Nullable;

/**
 * Parâmetros de tempo e lote do {@link Engine} — snapshot imutável no
 * lugar de parâmetros posicionais de construtor que só cresciam (Long
 * Parameter List). {@code leaseTtl} é o mesmo valor que alimenta o claim
 * ({@code mohs.engine.lease-ttl}): a renovação de lease (ADR-0012)
 * reescreve a lease com o mesmo horizonte com que ela nasce.
 * {@code watchdogTimeout} é o teto opcional da renovação (Watchdog
 * Bound, ADR-0012): {@code null} = sem teto; quando presente, precisa
 * ser maior que a lease — um bound menor tornaria a renovação inútil
 * (a primeira lease já nasceria condenada). O bound mede
 * submit→agora em tempo monotônico: espera na fila de um runner CPU
 * conta como runtime — semântica deliberada até o interrupt por
 * timeout de job (próximo ciclo) trazer um carimbo do início real.
 * {@code misfireThreshold} (ADR-0035) separa disparo atrasado de disparo
 * perdido: ocorrência devida dentro do threshold dispara atrasada em
 * qualquer política; mais velha que ele responde ao {@code Misfire} do
 * job.
 */
public record EngineSettings(Duration pollInterval, int batchSize, Duration leaseTtl, @Nullable Duration watchdogTimeout,
        Duration misfireThreshold) {

    /** Mesmo default de {@code mohs.engine.misfire-threshold} ({@code MohsProperties}) — precedente Quartz. */
    public static final Duration DEFAULT_MISFIRE_THRESHOLD = Duration.ofSeconds(60);

    public EngineSettings {
        Objects.requireNonNull(pollInterval, "pollInterval");
        Objects.requireNonNull(leaseTtl, "leaseTtl");
        Objects.requireNonNull(misfireThreshold, "misfireThreshold");
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be positive");
        }
        if (!pollInterval.isPositive()) {
            throw new IllegalArgumentException("mohs.engine.poll-interval must be positive, got " + pollInterval);
        }
        if (!leaseTtl.isPositive()) {
            throw new IllegalArgumentException("mohs.engine.lease-ttl must be positive, got " + leaseTtl
                    + " — a non-positive lease is born expired and turns the first tick into a reclaim storm");
        }
        if (watchdogTimeout != null && watchdogTimeout.compareTo(leaseTtl) <= 0) {
            throw new IllegalArgumentException("mohs.engine.watchdog-timeout (" + watchdogTimeout
                    + ") must be greater than mohs.engine.lease-ttl (" + leaseTtl
                    + ") — the bound is the ceiling ON TOP of renewal (ADR-0012), not a shorter lease");
        }
        if (!misfireThreshold.isPositive()) {
            throw new IllegalArgumentException("mohs.engine.misfire-threshold must be positive, got " + misfireThreshold
                    + " — a non-positive threshold turns every normally-late fire into a misfire");
        }
    }

    /** Threshold de misfire default (ADR-0035). */
    public EngineSettings(Duration pollInterval, int batchSize, Duration leaseTtl, @Nullable Duration watchdogTimeout) {
        this(pollInterval, batchSize, leaseTtl, watchdogTimeout, DEFAULT_MISFIRE_THRESHOLD);
    }

    /** Sem Watchdog Bound — renovação sem teto (default da ADR-0012) — e threshold de misfire default (ADR-0035). */
    public EngineSettings(Duration pollInterval, int batchSize, Duration leaseTtl) {
        this(pollInterval, batchSize, leaseTtl, null);
    }
}
