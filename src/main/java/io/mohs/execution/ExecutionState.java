package io.mohs.execution;

import io.mohs.event.ExecutionEvent;

/**
 * Estado de ciclo de vida de uma {@link Execution}. Não enumerado
 * explicitamente nos documentos de design; derivado aqui das variantes de
 * {@link ExecutionEvent} e do filtro {@code GET /executions?status=} do
 * design REST — ver {@code docs/adr/0010-rest-api-v1.md}.
 */
public enum ExecutionState {
    ENQUEUED,
    RUNNING,
    RETRY_SCHEDULED,
    SUCCEEDED,
    FAILED,
    CANCELLED
}
