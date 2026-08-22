package io.mohs.core;

import java.util.Objects;

/**
 * Leitura de um lote — o que {@code GET /batches/{id}} devolve e o que
 * {@link Mohs#findBatch} entrega a quem faz polling. Mesmo papel que
 * {@link RateLimitSnapshot} cumpre para um limite: tipo público de leitura,
 * separado do contador interno que o motor incrementa.
 *
 * <p>{@link #pending()} é derivado, não guardado: uma quarta coluna poderia
 * divergir das outras três, e não há pergunta que ela responda mais rápido
 * (ADR-0043).
 */
public record BatchSnapshot(String batchId, int total, int succeeded, int failed) {

    public BatchSnapshot {
        Objects.requireNonNull(batchId, "batchId");
        if (batchId.isBlank()) {
            throw new IllegalArgumentException("batchId must not be blank");
        }
        if (total < 0 || succeeded < 0 || failed < 0) {
            throw new IllegalArgumentException("counters must not be negative");
        }
        if (succeeded + failed > total) {
            throw new IllegalArgumentException("succeeded + failed must not exceed total");
        }
    }

    /** Membros que ainda não chegaram a um estado terminal. */
    public int pending() {
        return total - succeeded - failed;
    }

    /** {@code true} quando nenhum membro está pendente — o lote não reabre. */
    public boolean completed() {
        return pending() == 0;
    }
}
