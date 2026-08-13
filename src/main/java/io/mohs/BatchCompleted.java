package io.mohs;

import java.util.Objects;

/**
 * Dupla função deliberada, no mesmo espírito de {@link Enqueued}: é tanto o
 * dado passado para {@link Batch#onCompletion} quanto a variante de
 * {@link ExecutionEvent} correspondente — evita duplicar o mesmo formato
 * sob dois nomes.
 */
public record BatchCompleted(String batchId, int total, int succeeded, int failed) implements ExecutionEvent {

    public BatchCompleted {
        Objects.requireNonNull(batchId, "batchId");
        if (batchId.isBlank()) {
            throw new IllegalArgumentException("batchId must not be blank");
        }
        if (total < 0 || succeeded < 0 || failed < 0) {
            throw new IllegalArgumentException("counters must not be negative");
        }
    }
}
