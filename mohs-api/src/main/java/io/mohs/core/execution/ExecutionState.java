package io.mohs.core.execution;

import io.mohs.core.event.ExecutionEvent;

/**
 * Estado de ciclo de vida de uma {@link Execution}. Não enumerado
 * explicitamente nos documentos de design; derivado aqui das variantes de
 * {@link ExecutionEvent} e do filtro {@code GET /executions?status=} do
 * design REST — ver {@code docs/adr/0010-rest-api-v1.md}.
 *
 * <p>{@code RETRY_WAITING} (era {@code RETRY_SCHEDULED} até a Phase 5 do
 * redesign, §16.3-3) é a execução aguardando o backoff de um retry — e
 * <b>não é claimável</b>: quando o backoff vence ela volta a
 * {@link #ENQUEUED}, e a fila tem uma única regra de admissão
 * ({@code visible_at <= now}, §4.3/§5.8 do redesign). O rename carrega a
 * mudança de semântica: "scheduled" prometia um estado que o claim lia;
 * "waiting" diz o que ele é — uma espera.
 */
public enum ExecutionState {
    ENQUEUED,
    RUNNING,
    RETRY_WAITING,
    SUCCEEDED,
    FAILED,
    CANCELLED
}
