package io.mohs.rest.error;

import java.io.Serial;
import java.util.Objects;

/**
 * Payload incompatível com o tipo esperado pela definição do job — aponta
 * o campo problemático (ver {@code docs/REST-API-DESIGN.md}: "incompatível
 * → 422 problem+json apontando o campo").
 */
public final class PayloadValidationException extends RuntimeException {

    @Serial
    private static final long serialVersionUID = 1L;

    private final String field;

    public PayloadValidationException(String field, String message) {
        super(Objects.requireNonNull(message, "message"));
        this.field = Objects.requireNonNull(field, "field");
    }

    public String field() {
        return field;
    }
}
