package io.mohs.rest.batch;

import java.util.Objects;

/**
 * Contadores agregados de um lote — superset de
 * {@link io.mohs.core.event.BatchCompleted} (que só existe quando o lote
 * já terminou): aqui {@code pending = total - succeeded - failed} e o
 * lote pode ainda estar {@link BatchState#RUNNING}, já que o ponto de um
 * {@code GET} é permitir polling antes da conclusão também.
 */
public record BatchResponse(String batchId, BatchState state, int total, int succeeded, int failed, int pending) {

    public BatchResponse {
        Objects.requireNonNull(batchId, "batchId");
        Objects.requireNonNull(state, "state");
        if (total < 0 || succeeded < 0 || failed < 0 || pending < 0) {
            throw new IllegalArgumentException("counters must not be negative");
        }
    }

    /**
     * Deriva {@code pending} e {@code state} dos três contadores (DUP-4) —
     * mesmo padrão de fábrica estática de {@code JobDefinition#of}/
     * {@code ExecutionId#of}. Preferível ao construtor canônico sempre que
     * os contadores vêm de uma fonte só (ex.: {@code BatchCounters} do
     * motor), que nunca deveria poder divergir de {@code pending} por
     * construção.
     */
    public static BatchResponse of(String batchId, int total, int succeeded, int failed) {
        int pending = total - succeeded - failed;
        BatchState state = pending == 0 ? BatchState.COMPLETED : BatchState.RUNNING;
        return new BatchResponse(batchId, state, total, succeeded, failed, pending);
    }
}
