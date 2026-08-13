package io.mohs.engine;

import java.util.Objects;

/**
 * Contadores agregados de um lote — {@link #pending()} é derivado, nunca
 * persistido. Tipo interno do motor; {@code io.mohs.rest.batch.BatchResponse}
 * é a forma de wire equivalente, sem depender deste (direção errada).
 */
public record BatchCounters(String batchId, int total, int succeeded, int failed) {

    public BatchCounters {
        Objects.requireNonNull(batchId, "batchId");
        if (total < 0 || succeeded < 0 || failed < 0) {
            throw new IllegalArgumentException("counters must not be negative");
        }
    }

    public int pending() {
        return total - succeeded - failed;
    }
}
