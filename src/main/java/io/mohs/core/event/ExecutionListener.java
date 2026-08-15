package io.mohs.core.event;

/**
 * Observer (GoF) dos eventos de execução — observa, nunca interfere.
 * Exceção de listener é capturada e logada pelo motor; nunca afeta o job. Entrega é best-effort e assíncrona; reação garantida não usa
 * listener — o handler enfileira a continuação dentro da própria transação
 * (cláusula 4 do contrato assíncrono,
 * {@code docs/adr/0003-async-and-transactional-contract.md}).
 */
@FunctionalInterface
public interface ExecutionListener {
    void on(ExecutionEvent event);
}
