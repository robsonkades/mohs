package io.mohs.engine;

/**
 * A fronteira transacional que o engine NÃO pode abrir sozinho (sem
 * Spring-tx/JDBC neste módulo, §18.1): a unidade de enqueue do §7.5-1 —
 * {@code HistoryStore.record} + {@code WorkQueue.offer} — precisa de UMA
 * transação, e a ADR-0003 §4 exige que ela se junte à transação do host
 * quando existe. Esta porta é o Unit of Work (PoEAA) mínimo pra isso:
 * {@code io.mohs.store.jdbc} implementa com propagação {@code REQUIRED}
 * — junta-se à transação ativa do chamador, ou abre e commita uma própria.
 *
 * <p>Não é um {@code TransactionTemplate} genérico de aluguel: o único
 * chamador legítimo é a fachada compondo a unidade de enqueue (avulso,
 * lote, ocorrências não passam por aqui — o firer tem transação própria).
 * Exceção dentro de {@code work} aborta a unidade inteira e propaga.
 */
public interface StoreTransactions {

    void inTransaction(Runnable work);
}
