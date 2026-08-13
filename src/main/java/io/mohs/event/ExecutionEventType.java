package io.mohs.event;

/**
 * Espelha, um a um, as variantes de {@link ExecutionEvent} — existe só
 * porque {@link OnExecution#event()}, sendo atributo de annotation, não
 * pode referenciar diretamente qual record sealed permitido se quer
 * filtrar.
 */
public enum ExecutionEventType {
    ENQUEUED,
    STARTED,
    ATTEMPT_FAILED,
    RETRY_SCHEDULED,
    SUCCEEDED,
    FAILED,
    CANCELLED,
    BATCH_COMPLETED
}
