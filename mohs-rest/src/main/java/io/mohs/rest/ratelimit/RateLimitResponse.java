package io.mohs.rest.ratelimit;

import java.time.Duration;
import java.util.Objects;

import io.mohs.core.RateLimitSnapshot;

/**
 * Forma de wire de {@link io.mohs.core.resource.RateLimit} com o saldo
 * corrente do balde (ADR-0042). {@code available} — tokens disponíveis —
 * e não "usados": quem abre o dashboard quer saber quanto ainda cabe
 * AGORA, e "usado" nem é grandeza de balde (o refill é contínuo, não tem
 * virada de janela onde zerar um contador).
 */
public record RateLimitResponse(String name, int max, Duration window, int available) {

    public RateLimitResponse {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(window, "window");
    }

    static RateLimitResponse from(RateLimitSnapshot snapshot) {
        return new RateLimitResponse(snapshot.rateLimit().name(), snapshot.rateLimit().max(),
                snapshot.rateLimit().window(), snapshot.available());
    }
}
