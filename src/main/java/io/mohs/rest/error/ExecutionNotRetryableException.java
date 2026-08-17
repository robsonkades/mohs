package io.mohs.rest.error;

import java.io.Serial;

/**
 * {@code POST /executions/{id}/retry} sobre execução que existe mas não
 * está {@code FAILED} — 409: o recurso está num estado que conflita com a
 * transição pedida (retry manual só reconhece {@code FAILED}; ver
 * {@code Mohs#retry}). A mensagem carrega o estado atual — o 409 deve
 * ensinar, não só recusar.
 */
public final class ExecutionNotRetryableException extends RuntimeException {

    @Serial
    private static final long serialVersionUID = 1L;

    public ExecutionNotRetryableException(String message) {
        super(message);
    }
}
