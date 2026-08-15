package io.mohs.engine;

import java.time.Duration;
import java.util.Objects;

/**
 * Parâmetros de tempo e lote do {@link Engine} — snapshot imutável no
 * lugar de parâmetros posicionais de construtor que só cresciam (Long
 * Parameter List). {@code leaseTtl} é o mesmo valor que alimenta o claim
 * ({@code mohs.engine.lease-ttl}): a renovação de lease (ADR-0012)
 * reescreve a lease com o mesmo horizonte com que ela nasce.
 */
public record EngineSettings(Duration pollInterval, int batchSize, Duration leaseTtl) {

    public EngineSettings {
        Objects.requireNonNull(pollInterval, "pollInterval");
        Objects.requireNonNull(leaseTtl, "leaseTtl");
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be positive");
        }
    }
}
