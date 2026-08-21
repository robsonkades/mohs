package io.mohs.engine;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mohs.core.event.BatchCompleted;
import io.mohs.core.event.ExecutionEvent;
import io.mohs.core.event.ExecutionListener;

/**
 * As continuações registradas por {@code Batch.onCompletion} — um
 * {@link ExecutionListener} como qualquer outro, e não um caminho paralelo:
 * o {@code BatchCompleted} que dispara os callbacks é exatamente o evento
 * que o {@code Dispatcher} publica quando a conclusão fecha o lote
 * (ADR-0043), então a entrega herda o contrato de listener — assíncrona,
 * best-effort, sem garantia de ordem (ADR-0003).
 *
 * <p>Best-effort de verdade: o registro vive só nesta JVM. Um lote fechado
 * por OUTRO nó publica o evento lá, não aqui, e o callback não roda. Quem
 * precisa de reação garantida enfileira job dentro da transação — está no
 * Javadoc de {@code Batch#onCompletion} e é o motivo de este registro poder
 * ser um mapa em memória em vez de estado persistido.
 *
 * <p>O callback sai do mapa quando o lote fecha, e um lote nunca reabre
 * ({@code BatchCompleted} é terminal), então o mapa não cresce com o
 * tráfego — só com lotes vivos que alguém observou. Callback que lança é
 * logado e engolido: um observador quebrado não pode derrubar a entrega
 * dos demais, nem o evento para os outros listeners.
 */
public final class BatchCompletionCallbacks implements ExecutionListener {

    private static final Logger log = LoggerFactory.getLogger(BatchCompletionCallbacks.class);

    private final ConcurrentHashMap<String, List<Consumer<BatchCompleted>>> byBatchId = new ConcurrentHashMap<>();

    void register(String batchId, Consumer<BatchCompleted> callback) {
        Objects.requireNonNull(batchId, "batchId");
        Objects.requireNonNull(callback, "callback");
        byBatchId.computeIfAbsent(batchId, _ -> new CopyOnWriteArrayList<>()).add(callback);
    }

    @Override
    public void on(ExecutionEvent event) {
        if (!(event instanceof BatchCompleted completed)) {
            return;
        }
        List<Consumer<BatchCompleted>> callbacks = byBatchId.remove(completed.batchId());
        if (callbacks == null) {
            return;
        }
        for (Consumer<BatchCompleted> callback : callbacks) {
            try {
                callback.accept(completed);
            } catch (RuntimeException e) {
                log.error("onCompletion callback for batch {} threw — the batch is complete regardless, "
                        + "and the remaining callbacks still run", completed.batchId(), e);
            }
        }
    }
}
