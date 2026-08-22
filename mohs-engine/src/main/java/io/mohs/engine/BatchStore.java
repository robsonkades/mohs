package io.mohs.engine;

import java.util.Optional;

/**
 * Persistência de contadores de lote — Repository (PoEAA), porta que
 * {@code io.mohs.store.jdbc} implementa. {@link #incrementSucceeded}/
 * {@link #incrementFailed} são incrementos atômicos no SQL (não
 * leia-then-escreva): execuções do mesmo lote completam concorrentemente
 * em threads diferentes.
 *
 * <p>Os dois devolvem o saldo PÓS-incremento porque é ele que elege quem
 * fecha o lote (ADR-0043): exatamente um chamador enxerga
 * {@link BatchCounters#pending()} zerado, e é esse que dispara
 * {@code BatchCompleted}. Perguntar depois, num {@link #find} separado,
 * não serve — duas conclusões concorrentes leriam o mesmo saldo final e as
 * duas se achariam a fechadora.
 */
public interface BatchStore {

    void insert(String batchId, int total);

    Optional<BatchCounters> find(String batchId);

    /**
     * @return o saldo do lote depois deste incremento; {@code pending() == 0}
     * identifica esta chamada como a que fechou o lote
     */
    BatchCounters incrementSucceeded(String batchId);

    /** @see #incrementSucceeded */
    BatchCounters incrementFailed(String batchId);
}
