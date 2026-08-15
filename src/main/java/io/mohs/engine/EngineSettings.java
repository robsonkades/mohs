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
 * (a primeira lease já nasceria condenada).
 */
public record EngineSettings(Duration pollInterval, int batchSize, Duration leaseTtl, @Nullable Duration watchdogTimeout) {

    public EngineSettings {
        Objects.requireNonNull(pollInterval, "pollInterval");
        Objects.requireNonNull(leaseTtl, "leaseTtl");
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be positive");
        }
        if (watchdogTimeout != null && watchdogTimeout.compareTo(leaseTtl) <= 0) {
            throw new IllegalArgumentException("mohs.engine.watchdog-timeout (" + watchdogTimeout
                    + ") must be greater than mohs.engine.lease-ttl (" + leaseTtl
                    + ") — the bound is the ceiling ON TOP of renewal (ADR-0012), not a shorter lease");
        }
    }

    /** Sem Watchdog Bound — renovação sem teto (default da ADR-0012). */
    public EngineSettings(Duration pollInterval, int batchSize, Duration leaseTtl) {
        this(pollInterval, batchSize, leaseTtl, null);
    }
}
