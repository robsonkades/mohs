package io.mohs.engine;

import java.util.List;
import java.util.Objects;

import io.mohs.core.execution.Execution;

/**
 * Reclama {@code Execution RUNNING} cuja lease expirou — nó morto,
 * partição de rede, ou Handler zumbi que ignora cancelamento (ADR-0012).
 * Não é Repository de uma entidade só: orquestra {@link ExecutionStore}
 * (grava o Attempt e transiciona o estado, via {@link
 * ExecutionStore#complete}) e {@link JobStore} (libera a vaga de
 * concorrência do job, ADR-0025) — mesmo motivo de {@link Claimer} não
 * ser um {@code *Store}.
 */
public interface Reaper {

    /**
     * Uma execução reclamada, já no estado final, mais o que só o reaper
     * sabe na hora da decisão: se o terminal veio de orçamento esgotado
     * ({@code Failed.attemptsExhausted}) ou de outra causa (job
     * aposentado) — a {@link Execution} sozinha não distingue.
     */
    record Reclaimed(Execution execution, boolean attemptsExhausted) {
        public Reclaimed {
            Objects.requireNonNull(execution, "execution");
        }
    }

    /**
     * Reclama {@code Execution RUNNING} com lease expirada, até o teto por
     * ciclo da implementação: grava um {@code Attempt} {@code FAILED}
     * sintético ("lease expirada — node presumido morto") e transiciona
     * para {@code RETRY_SCHEDULED} com backoff quando há orçamento
     * (ADR-0033) ou {@code FAILED} terminal quando não há (ou o job foi
     * aposentado). O teto limita o lote de uma morte de nó em massa —
     * comportamento definido em toda borda; o excedente drena nos ciclos
     * seguintes.
     *
     * @return as execuções cuja transição realmente ocorreu nesta chamada.
     */
    List<Reclaimed> reclaimExpired();
}
