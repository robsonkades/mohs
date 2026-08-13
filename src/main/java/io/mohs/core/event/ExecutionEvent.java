package io.mohs.core.event;

/**
 * Eventos observáveis do ciclo de vida de uma execução ou lote. Selado para
 * que {@link ExecutionListener} implementations façam pattern matching
 * exaustivo — uma variante nova em release futuro vira aviso do compilador
 * em todo listener existente, não um {@code default} silencioso.
 */
public sealed interface ExecutionEvent
        permits Enqueued, Started, AttemptFailed, RetryScheduled, Succeeded, Failed, Cancelled, BatchCompleted {
}
