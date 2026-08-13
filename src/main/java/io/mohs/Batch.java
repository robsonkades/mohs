package io.mohs;

import java.util.function.Consumer;

import io.mohs.event.BatchCompleted;
import io.mohs.event.ExecutionListener;

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

    Batch onCompletion(Consumer<BatchCompleted> callback);
}
