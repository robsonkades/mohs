package io.mohs.core.event;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link ExecutionEventType} espelha manualmente as variantes de {@link
 * ExecutionEvent} (cláusula {@code permits}) porque {@code
 * OnExecution#event()}, sendo atributo de annotation, não pode
 * referenciar o record selado diretamente. Nada no compilador barra os
 * dois de divergir silenciosamente se uma variante nova for adicionada
 * só de um lado — este teste é a paridade mecânica que falta.
 */
class ExecutionEventTypeTest {

    @Test
    void mirrorsEveryExecutionEventPermittedSubtype() {
        // ExecutionEvent.java: permits Enqueued, Started, AttemptFailed,
        // RetryScheduled, Succeeded, Failed, Cancelled, BatchCompleted
        assertThat(ExecutionEventType.values()).extracting(Enum::name).containsExactly(
                "ENQUEUED", "STARTED", "ATTEMPT_FAILED", "RETRY_WAITING",
                "SUCCEEDED", "FAILED", "CANCELLED", "BATCH_COMPLETED");
    }
}
