package io.mohs.rest.ratelimit;

import java.time.Duration;

import io.mohs.core.resource.RateLimit;

/**
 * Corpo de {@code PATCH /rate-limits/{name}} — valida pela MESMA regra de
 * {@link RateLimit#requireRefillable}, não por uma cópia: a divergência
 * entre a validação de wire e a do domínio é o que deixaria um
 * {@code window} curto demais atravessar a API e derrubar a rodada de
 * claim (ADR-0042). Aqui a falha é 422 na cara do cliente, no boot é erro
 * de startup.
 */
public record RateLimitPatchRequest(int max, Duration window) {

    public RateLimitPatchRequest {
        RateLimit.requireRefillable(max, window);
    }
}
