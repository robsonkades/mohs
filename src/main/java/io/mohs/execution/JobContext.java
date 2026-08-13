package io.mohs.execution;

import java.time.Instant;

import io.mohs.ExecutionId;
import io.mohs.JobKey;

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

    /** Cooperativo — respaldado por um cache do lado do motor com ~1s de staleness. */
    boolean cancellationRequested();

    /** Opcional, voltado ao dashboard. No-op se nada observar. */
    void progress(int done, int total);
}
