package io.mohs.engine;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.RejectedExecutionException;

import org.springframework.core.task.AsyncTaskExecutor;

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
 * <p>{@code executor} é injetado, não criado aqui — {@link MohsExecutors}
 * é quem sabe construir um virtual-thread executor com teto de concorrência
 * real (CLAUDE.md); esta classe só consome. Não fecha o executor recebido:
 * ciclo de vida é de quem o construiu (mesma disciplina de
 * {@link MohsExecutors}).
 *
 * <p>Package-private: único consumidor hoje é {@link Dispatcher}. Abre pra
 * {@code public} no dia que outro código (ex.: retry manual via REST)
 * precisar publicar evento também — YAGNI até lá.
 */
final class ExecutionEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(ExecutionEventPublisher.class);

    private final List<ExecutionListener> listeners;
    private final AsyncTaskExecutor executor;

    ExecutionEventPublisher(List<ExecutionListener> listeners, AsyncTaskExecutor executor) {
        this.listeners = List.copyOf(Objects.requireNonNull(listeners, "listeners"));
        this.executor = Objects.requireNonNull(executor, "executor");
    }

    /**
     * O pipeline de observação nunca exerce backpressure sobre o de
     * controle: executor saturado descarta o evento com WARN aqui — a borda
     * que descarta é o publisher, nunca uma exceção subindo pelo poll loop
     * (um reclaim em massa publicando 2×500 eventos não pode sequestrar o
     * tick) nem pelo caminho de conclusão do dispatch.
     */
    void publish(ExecutionEvent event) {
        Objects.requireNonNull(event, "event");
        for (ExecutionListener listener : listeners) {
            try {
                executor.execute(() -> {
                    try {
                        listener.on(event);
                    } catch (RuntimeException e) {
                        log.warn("execution listener {} threw for event {} — ignored, listener exceptions never affect the job", listener.getClass().getName(), event, e);
                    }
                });
            } catch (RejectedExecutionException e) {
                log.warn("event executor saturated — dropping {} for listener {} (delivery is best-effort by contract)",
                        event.getClass().getSimpleName(), listener.getClass().getName());
            }
        }
    }
}
