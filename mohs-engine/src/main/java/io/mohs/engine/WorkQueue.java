package io.mohs.engine;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import io.mohs.core.execution.ExecutionId;
import io.mohs.core.job.JobKey;

/**
 * A FILA ({@code mohs_ready}) e a transição fila→posse — porta da Phase 5
 * (ADR-A, §5.3/§5.4/§18.3 do redesign), uma das quatro que substituem as
 * oito da era da tabela única. O tamanho da fila é o BACKLOG, nunca a
 * história: entrada nasce no enqueue/retry/requeue ({@link #offer}) e
 * morre no claim ({@link #claim}) — que, na mesma transação, insere a
 * posse em {@code mohs_lease} (§6.2: não existe instante em que uma
 * execução não esteja nem na fila nem possuída; quem garante é o storage,
 * não a ordem de chamadas da aplicação).
 *
 * <p>Retry, delayed, requeue de reaper e enqueue imediato são a MESMA
 * operação com {@code visibleAt} diferente (§5.8) — uma regra de
 * visibilidade ({@code visible_at <= now}), zero estados: é o que tirou o
 * retry do predicado de claim ({@code RETRY_WAITING} não é claimável) e
 * matou a regressão de 3× do {@code IN} de dois valores no MySQL (§4.3).
 */
public interface WorkQueue {

    /**
     * Uma entrada da fila. {@code attempt} é o attempt que esta entrada
     * VAI virar quando reivindicada (1 no primeiro enqueue; anterior + 1
     * em retry/requeue) — o claim o copia pra
     * {@code mohs_lease.attempt_number} e nada mais conta attempts no
     * caminho quente (§5.3). {@code shard} vem de {@link Shards#of} desde
     * a Phase 6 (ADR-F); a faixa é validada AQUI, onde o dado entra no
     * tipo (Effective Java 49): uma linha gravada fora de [0, 64) nunca
     * seria reivindicada — o lap só sonda shards que a partição derivada
     * distribui, e a entrada apodreceria na fila em silêncio.
     */
    record ReadyEntry(ExecutionId executionId, JobKey jobKey, int shard, int priority, int attempt, Instant visibleAt) {
        public ReadyEntry {
            Objects.requireNonNull(executionId, "executionId");
            Objects.requireNonNull(jobKey, "jobKey");
            Objects.requireNonNull(visibleAt, "visibleAt");
            if (attempt < 1) {
                throw new IllegalArgumentException("attempt must be >= 1 — it is the attempt this entry becomes, not a counter");
            }
            if (shard < 0 || shard >= Shards.SHARD_COUNT) {
                throw new IllegalArgumentException("shard must be in [0, " + Shards.SHARD_COUNT
                        + ") — no node's derived partition would ever probe it and the entry would rot unclaimed");
            }
        }
    }

    /** O que o claim devolve: identidade, nunca payload (§5.4 — o dispatcher segue com UMA leitura em lote na história). {@code priority} viaja de volta pro requeue de perda de admissão reconstruir a entrada sem leitura extra. */
    record ClaimedWork(ExecutionId executionId, JobKey jobKey, int attemptNumber, int priority) {
        public ClaimedWork {
            Objects.requireNonNull(executionId, "executionId");
            Objects.requireNonNull(jobKey, "jobKey");
        }
    }

    /**
     * O claim (§5.4): numa transação, remove até {@code limit} entradas
     * visíveis do shard — ordenadas por {@code (priority, visible_at)},
     * pulando linhas travadas por claims concorrentes — e insere a posse
     * {@code (nodeId, epoch)} em {@code mohs_lease}. UM shard por chamada,
     * nunca uma lista (lição do E2: predicado multi-shard mata a ordenação
     * do índice — 25,5 ms/round contra 0,43 ms, BASELINE); o chamador faz
     * o round-robin dos shards que possui.
     *
     * <p>{@code inadmissible} é a lista node-local de job keys excluídos
     * NESTA rodada (janela fechada, cap sem folga, semáforo de rate limit
     * vazio, handler ausente) — computada em memória ANTES da rodada, por
     * job e não por candidato (§5.4, "What happened to the guards").
     * Snapshot deliberado: um guard que vira no meio da rodada é resolvido
     * no dispatch (admissão perde → requeue), não aqui.
     *
     * <p>A lista devolvida vem NA ORDEM {@code (priority, visible_at)} nos
     * quatro dialetos (contrato, não acidente — review S5.2). Invariante de
     * dimensionamento: {@code limit} ≤ folga de dispatch (~1k) &lt; 2000 —
     * o teto de parâmetros do SQL Server; a forma portátil não fatia o
     * {@code IN} do DELETE de propósito.
     */
    List<ClaimedWork> claim(int shard, String nodeId, long epoch, int limit, Collection<JobKey> inadmissible, Instant now);

    /**
     * Insere entradas na fila. NÃO abre transação própria: o chamador DEVE
     * compor {@code HistoryStore.record} + {@code offer} numa única
     * transação (§7.5-1; ADR-0003 §4 — junta-se à do host quando existe, e
     * a facade abre a sua, {@code REQUIRED}, quando não). Chamar fora de
     * transação quebra a unidade do enqueue: falha parcial deixa chave de
     * idempotência órfã (dedup contra o nada pela janela inteira) ou
     * execução {@code PENDING} inalcançável — não é um modo suportado.
     */
    void offer(List<ReadyEntry> entries);

    /**
     * O caminho de recuperação/perda de admissão (§4.3/§5.4): numa
     * transação, deleta a lease indicada — cercada por
     * {@code (node_id, epoch)} da encarnação OBSERVADA, o fencing token do
     * §6.3 — e reinsere a entrada na fila. Perder o fence (a lease já não
     * existe ou trocou de dono) pula a reinserção daquela entrada: a
     * encarnação nova é a autoridade. Devolve quantas entradas
     * efetivamente voltaram pra fila.
     */
    int requeue(List<Requeue> orders);

    /** Ordem de requeue: a lease a derrubar (com o fence observado) e a entrada que renasce na fila. */
    record Requeue(ExecutionId executionId, String nodeId, long epoch, ReadyEntry entry) {
        public Requeue {
            Objects.requireNonNull(executionId, "executionId");
            Objects.requireNonNull(nodeId, "nodeId");
            Objects.requireNonNull(entry, "entry");
        }
    }

    /**
     * Cancela uma execução ainda na FILA (ADR-0034, metade "pendente"):
     * numa transação, remove a entrada e grava o terminal advisory
     * {@code CANCELLED} na história. Perde pra qualquer claim concorrente
     * (a entrada já saiu da fila) — o chamador cai então na flag
     * cooperativa da posse ({@code LeaseStore#requestCancellation}).
     *
     * @return {@code true} se ESTA chamada removeu a entrada
     */
    boolean cancelQueued(ExecutionId id, Instant now);

    /**
     * Retry manual do operador (ADR-0033 na mesa nova): numa transação,
     * CAS do advisory {@code FAILED → PENDING} — guardado por job não
     * aposentado, o mesmo guard do CAS antigo — e renascimento na fila com
     * {@code attempt = attempts gravados + 1} e a prioridade original.
     * Bypassa o orçamento de propósito: quem decide é o operador.
     *
     * @return {@code true} se ESTA chamada armou; {@code false} = id
     *         inexistente, estado ≠ {@code FAILED} ou job aposentado — o
     *         chamador distingue com uma leitura
     */
    boolean rearmForManualRetry(ExecutionId id, Instant now);
}
