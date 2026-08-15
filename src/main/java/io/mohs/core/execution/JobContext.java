package io.mohs.core.execution;

import java.time.Instant;

import io.mohs.core.job.JobKey;

/**
 * Parâmetro opcional do handler carregando contexto por tentativa:
 * identidade, timing, cancelamento cooperativo e relato de progresso.
 * Deliberadamente uma interface simples, não fluente — isso vive no
 * caminho quente (à parte da disciplina de Effective Java, um DSL aqui só
 * poluiria stack trace).
 */
public interface JobContext {

    JobKey jobKey();

    ExecutionId executionId();

    /** 1-based; retry incrementa isto, o id da execução permanece o mesmo. */
    int attempt();

    Instant scheduledAt();

    Instant firedAt();

    /**
     * Cooperativo (ADR-0034) — vira {@code true} quando o timeout do job
     * dispara, o shutdown escala no estouro do grace de drain, ou um
     * {@code POST /executions/{id}/cancel} é observado pelo node dono
     * (staleness ≤ 1 poll-interval, {@code mohs.engine.poll-interval}).
     * O handler decide quando e como parar: sair com exceção depois de
     * observar um cancel manual encerra a execução como {@code CANCELLED};
     * concluir normalmente registra {@code SUCCEEDED} — trabalho terminado
     * vale, mesmo com pedido pendente. Timeout e shutdown também entregam
     * {@code Thread.interrupt()}; o cancel manual, não — é flag pura.
     */
    boolean cancellationRequested();

    /** Opcional, voltado ao dashboard. No-op se nada observar. */
    void progress(int done, int total);
}
