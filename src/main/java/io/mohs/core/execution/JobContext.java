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
     * Cooperativo — <b>nesta versão sempre {@code false}</b>: ainda não
     * existe fonte de cancelamento ({@code POST /executions/{id}/cancel}
     * responde 501). O design final usa um cache do lado do motor com ~1s
     * de staleness.
     */
    boolean cancellationRequested();

    /** Opcional, voltado ao dashboard. No-op se nada observar. */
    void progress(int done, int total);
}
