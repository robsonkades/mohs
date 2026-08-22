package io.mohs.engine;

import java.time.Instant;
import java.util.List;

import org.jspecify.annotations.Nullable;

import io.mohs.core.execution.Execution;
import io.mohs.core.job.JobKey;

/**
 * Dispara um trigger devido (ADR-0035) — a etapa "trigger devido →
 * aquisição" do fluxo de job. Não é Repository de uma entidade só: avança
 * {@code mohs_job_definitions.next_fire_at} e insere em
 * {@code mohs_executions} numa única transação — porta própria, mesmo
 * padrão de {@link Claimer} ({@code io.mohs.store.jdbc} implementa).
 *
 * <p>A exclusão mútua cluster-wide é o CAS do avanço: {@code UPDATE ...
 * WHERE next_fire_at = :observado} — só o nó que vence insere as
 * ocorrências, e avanço + inserção atômicos garantem que crash entre os
 * dois não perde nem duplica ocorrência. Por isso as ocorrências não
 * carregam Idempotency-Key (que ficaria sujeita à janela de retenção,
 * ADR-0030).
 */
public interface TriggerFirer {

    /**
     * CAS-avança {@code next_fire_at} de {@code observedNextFireAt} para
     * {@code newNextFireAt} ({@code null} = desarma — fixed-delay
     * aguardando o fim) e, vencendo, insere {@code occurrences} com
     * {@code payload} na mesma transação.
     *
     * @return {@code true} se ESTA chamada avançou o trigger (e inseriu);
     *         {@code false} se outro nó venceu a corrida — nada inserido.
     */
    boolean fire(JobKey key, Instant observedNextFireAt, @Nullable Instant newNextFireAt,
            List<Execution> occurrences, Object payload);
}
