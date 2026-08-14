package io.mohs.engine;

import java.util.Optional;

/**
 * Persistência de contadores de lote — Repository (PoEAA), porta que
 * {@code io.mohs.jdbc} implementa. {@link #incrementSucceeded}/
 * {@link #incrementFailed} são incrementos atômicos no SQL (não
 * leia-then-escreva): execuções do mesmo lote completam concorrentemente
 * em threads diferentes.
 */
public interface BatchStore {

    void insert(String batchId, int total);

    Optional<BatchCounters> find(String batchId);

    void incrementSucceeded(String batchId);

    void incrementFailed(String batchId);
}
