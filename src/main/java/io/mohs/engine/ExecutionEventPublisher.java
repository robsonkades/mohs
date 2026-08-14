package io.mohs.engine;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mohs.core.event.ExecutionEvent;
import io.mohs.core.event.ExecutionListener;

/**
 * Entrega {@link ExecutionEvent} aos {@link ExecutionListener} registrados —
 * best-effort e assíncrona, numa virtual thread dedicada por publicação
 * (contrato já documentado em {@link ExecutionListener}: "observa, nunca
 * interfere"). Exceção de listener é capturada e logada, nunca propaga —
 * um listener quebrado não pode afetar o resultado do job.
 *
 * <p>Package-private: único consumidor hoje é {@link Dispatcher}. Abre pra
 * {@code public} no dia que outro código (ex.: retry manual via REST)
 * precisar publicar evento também — YAGNI até lá.
 */
final class ExecutionEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(ExecutionEventPublisher.class);

    private final List<ExecutionListener> listeners;
    private final ExecutorService executor;

    ExecutionEventPublisher(List<ExecutionListener> listeners) {
        this.listeners = List.copyOf(Objects.requireNonNull(listeners, "listeners"));
        this.executor = Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("mohs-events-", 0).factory());
    }

    void publish(ExecutionEvent event) {
        Objects.requireNonNull(event, "event");
        for (ExecutionListener listener : listeners) {
            executor.execute(() -> {
                try {
                    listener.on(event);
                } catch (RuntimeException e) {
                    log.warn("execution listener {} threw for event {} — ignored, listener exceptions never affect the job",
                            listener.getClass().getName(), event, e);
                }
            });
        }
    }
}
