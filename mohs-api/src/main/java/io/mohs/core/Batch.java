package io.mohs.core;

import java.util.function.Consumer;

import io.mohs.core.event.BatchCompleted;
import io.mohs.core.event.ExecutionListener;

/**
 * Recibo de um lote — {@link #batchId()} já está disponível de forma
 * síncrona, mesma durabilidade da cláusula 2 do contrato assíncrono
 * ({@code docs/adr/0003-async-and-transactional-contract.md}).
 * {@link #onCompletion} registra uma continuação best-effort, no mesmo
 * espírito de {@link ExecutionListener} — reação garantida enfileira job
 * dentro da própria transação, não depende deste callback.
 */
public interface Batch {

    String batchId();

    /**
     * Registra um callback best-effort disparado no fim do lote (êxito ou
     * falha) — não é a garantia de conclusão do batch, só uma notificação
     * de conveniência. Cada chamada registra um listener independente, sem
     * substituir os já registrados: {@code batch.onCompletion(a).onCompletion(b)}
     * registra os dois, não só o último. O {@link Batch} retornado
     * referencia o mesmo lote, nunca uma cópia.
     */
    Batch onCompletion(Consumer<BatchCompleted> callback);
}
