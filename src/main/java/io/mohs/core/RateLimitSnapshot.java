package io.mohs.core;

import java.util.Objects;

import io.mohs.core.resource.RateLimit;

/**
 * Um {@link RateLimit} mais o estado corrente do balde de tokens que o
 * aplica (ADR-0042) — a forma de leitura devolvida por
 * {@link Mohs#rateLimits}. Mesma composição de {@link JobSnapshot}: a
 * spec é o que foi declarado, {@code available} é o que o motor tem
 * agora.
 *
 * @param available tokens disponíveis no instante da leitura — quantos disparos cabem AGORA, não quantos foram usados; um balde cheio vale {@code rateLimit.max()}
 */
public record RateLimitSnapshot(RateLimit rateLimit, int available) {

    public RateLimitSnapshot {
        Objects.requireNonNull(rateLimit, "rateLimit");
        if (available < 0) {
            throw new IllegalArgumentException("available must not be negative, got " + available);
        }
        if (available > rateLimit.max()) {
            throw new IllegalArgumentException(
                    "available must not exceed the bucket capacity of " + rateLimit.max() + ", got " + available);
        }
    }
}
