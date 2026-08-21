package io.mohs.rest.error;

import java.io.Serial;

/**
 * Actor declarado na request não cabe no contrato de persistência — ex.:
 * header {@code X-Mohs-Actor} maior que a coluna {@code actor}
 * ({@code VARCHAR(255)}). Validado na borda pra virar 400 com detail que
 * ensina, nunca uma falha de {@code INSERT} respondida como 500.
 */
public final class InvalidActorException extends RuntimeException {

    @Serial
    private static final long serialVersionUID = 1L;

    public InvalidActorException(String message) {
        super(message);
    }
}
