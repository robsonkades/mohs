package io.mohs.core.event;

/**
 * Observer (GoF) dos eventos de execução — observa, nunca interfere.
 * Exceção de listener é capturada e logada pelo motor; nunca afeta o job. Entrega é best-effort e assíncrona; reação garantida não usa
 * listener — o handler enfileira a continuação dentro da própria transação
 * (cláusula 4 do contrato assíncrono,
 * {@code docs/adr/0003-async-and-transactional-contract.md}).
 *
 * <p>Limitações desta rodada, documentadas: os eventos refletem só o ciclo
 * de dispatch local — desfechos decididos pelo reaper (reclaim de lease
 * expirada, inclusive os retries/falhas terminais de morte de nó) ainda
 * não publicam nada; e não há garantia de ordem entre eventos da mesma
 * execução (entrega assíncrona por listener — {@code RetryScheduled} pode
 * chegar antes do {@code AttemptFailed} que causalmente o precede).
 */
@FunctionalInterface
public interface ExecutionListener {
    void on(ExecutionEvent event);
}
