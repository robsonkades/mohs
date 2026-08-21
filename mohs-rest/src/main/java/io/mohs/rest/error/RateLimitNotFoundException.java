package io.mohs.rest.error;

import java.io.Serial;
import java.util.Objects;

/**
 * {@code PATCH /rate-limits/{name}} contra um limite que não existe.
 * Ajustar é ato de emergência sobre algo declarado; DECLARAR é ato de boot
 * (ADR-0042/ADR-0006) — por isso 404, e não criação implícita: um limite
 * nascido de um PATCH sumiria no próximo deploy sem deixar rastro no
 * repositório.
 */
public final class RateLimitNotFoundException extends RuntimeException {

    @Serial
    private static final long serialVersionUID = 1L;

    private final String rateLimitName;

    public RateLimitNotFoundException(String rateLimitName) {
        super("Rate limit not found: " + rateLimitName);
        this.rateLimitName = Objects.requireNonNull(rateLimitName, "rateLimitName");
    }

    public String rateLimitName() {
        return rateLimitName;
    }
}
