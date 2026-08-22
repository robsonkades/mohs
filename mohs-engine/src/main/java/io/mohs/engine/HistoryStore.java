package io.mohs.engine;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.jspecify.annotations.Nullable;

import io.mohs.core.execution.Attempt;
import io.mohs.core.execution.Execution;
import io.mohs.core.execution.ExecutionId;
import io.mohs.core.execution.ExecutionState;
import io.mohs.core.job.JobKey;

/**
 * A HISTÓRIA ({@code mohs_execution}/{@code mohs_attempt}/
 * {@code mohs_idempotency}) — porta da Phase 5 (ADR-A, §7.2/§18.3 do
 * redesign). Append + UM update terminal por execução; no Tier 1 as
 * tabelas são particionadas por tempo e retenção é DROP de partição. O
 * {@code state} daqui é read model ADVISORY (§6.2): em voo, a verdade é
 * a lease — leituras que precisam de verdade juntam {@link LeaseStore},
 * leituras que precisam de velocidade (dashboard) usam a coluna e aceitam
 * a staleness limitada de um flush.
 *
 * <p>O UPDATE terminal e o INSERT do attempt NÃO passam por aqui — são da
 * transação de conclusão de {@link LeaseStore#complete} (§7.5-3): as
 * portas seguem os CONCEITOS (fila, posse, história, controle), e a
 * conclusão é um conceito da posse que toca a história, não o contrário.
 */
public interface HistoryStore {

    /**
     * Uma execução aceita, pronta pro registro de nascimento.
     * {@code createdAt} é o instante do enqueue e a CHAVE DE PARTIÇÃO —
     * viaja em memória até a conclusão (PLAN.md S5.1: a poda do UPDATE
     * terminal é por igualdade). {@code correlationId} carrega o batch
     * (ADR-0043) até a Phase 8 generalizar.
     */
    record NewExecution(ExecutionId executionId, JobKey jobKey, int shard, int priority, Instant scheduledAt,
            Instant createdAt, String actor, @Nullable String correlationId, @Nullable String idempotencyKey,
            Object payload) {
        public NewExecution {
            Objects.requireNonNull(executionId, "executionId");
            Objects.requireNonNull(jobKey, "jobKey");
            Objects.requireNonNull(scheduledAt, "scheduledAt");
            Objects.requireNonNull(createdAt, "createdAt");
            Objects.requireNonNull(actor, "actor");
            Objects.requireNonNull(payload, "payload");
        }
    }

    /**
     * Registro de nascimento (§7.5-1): {@code INSERT} na história com
     * {@code state = 'PENDING'} e, quando há {@code idempotencyKey},
     * {@code INSERT} em {@code mohs_idempotency} — cujo conflito de PK É o
     * check de dedup (Idempotent Receiver, EIP): propaga como
     * {@code DuplicateKeyException} pro chamador resolver a execução
     * vencedora via {@link #findByIdempotencyKey}. NÃO abre transação
     * própria: o chamador DEVE compor {@code record} + {@code
     * WorkQueue.offer} numa única transação (§7.5-1) — ver o contrato de
     * {@code WorkQueue#offer}; fora de transação, falha parcial deixa
     * chave órfã ou execução inalcançável. Com N execuções e UMA chave
     * duplicada, a unidade INTEIRA aborta — resolução por item é do
     * chamador (re-tentar sem o duplicado), não daqui.
     */
    void record(List<NewExecution> executions);

    /** Quem venceu a corrida de idempotência — o id gravado em {@code mohs_idempotency} para (job, chave); vazio quando a chave nunca foi usada (ou já foi podada). */
    Optional<ExecutionId> findByIdempotencyKey(JobKey jobKey, String idempotencyKey);

    /**
     * A leitura em lote que segue cada rodada de claim (§5.4): payload +
     * cabeçalho dos ids reivindicados (o dispatch precisa de
     * {@code scheduledAt}/{@code actor}/{@code priority}/{@code batchId}
     * pra eventos, rearme e conclusão — uma leitura só, nunca N). Infra e
     * linha separadas como na ADR-0047: falha de deserialização de UMA
     * linha não derruba o lote — a linha entra em {@code unreadable} e o
     * resto despacha.
     */
    PayloadBatch findPayloads(List<ExecutionId> ids);

    /** Payload hidratado + o cabeçalho da execução ({@code createdAt} poda a partição do UPDATE terminal; o resto alimenta dispatch/eventos/conclusão). */
    record PayloadRow(ExecutionHead head, Object payload) {
        public PayloadRow {
            Objects.requireNonNull(head, "head");
            Objects.requireNonNull(payload, "payload");
        }
    }

    /** O cabeçalho de uma linha de história — tudo menos payload e attempts; {@code correlationId} carrega o lote (ADR-0043) até a Phase 8. */
    record ExecutionHead(ExecutionId executionId, JobKey jobKey, Instant scheduledAt, Instant createdAt, String actor,
            int priority, @Nullable String correlationId) {
        public ExecutionHead {
            Objects.requireNonNull(executionId, "executionId");
            Objects.requireNonNull(jobKey, "jobKey");
            Objects.requireNonNull(scheduledAt, "scheduledAt");
            Objects.requireNonNull(createdAt, "createdAt");
            Objects.requireNonNull(actor, "actor");
        }
    }

    /** Mesma separação de {@code ExecutionStore.PayloadBatch} (ADR-0047): {@code unreadable} = linhas ilegíveis (falha PERMANENTE, com a causa pro attempt terminal), nunca infra — infra propaga como exceção da própria chamada. */
    record PayloadBatch(Map<ExecutionId, PayloadRow> rows, Map<ExecutionId, RuntimeException> unreadable) {
        public PayloadBatch {
            rows = Map.copyOf(rows);
            unreadable = Map.copyOf(unreadable);
        }
    }

    /** Só os cabeçalhos — o caminho frio do reaper (rearme/lote/poda dos candidatos terminais) sem pagar deserialização de payload. */
    List<ExecutionHead> findHeads(List<ExecutionId> ids);

    /** Attempts de uma execução, em ordem de número — o detail view (servido por {@code idx_mohs_attempt_exec} no PG). */
    List<Attempt> findAttempts(ExecutionId executionId);

    // ─── read model (§6.2): estado advisory + verdade da fila/posse ─────────

    /**
     * A execução como a API pública a vê — com attempts, e com o estado
     * DERIVADO: a coluna {@code state} da história é advisory (PENDING até
     * o terminal); em voo, a verdade é a fila e a posse. A derivação
     * (§4.3): lease presente → {@code RUNNING} (com {@code owner} =
     * node dono e {@code firedAt} = claim); entrada de fila com
     * {@code attempt > 1} ainda invisível → {@code RETRY_WAITING}; entrada
     * de fila caso contrário → {@code ENQUEUED}; coluna terminal → ela
     * mesma. Linha PENDING sem fila nem posse é a janela de um flush de
     * conclusão em curso — lê {@code ENQUEUED} (staleness limitada e
     * documentada do modelo, §6.2). {@code now} decide {@code ENQUEUED} ×
     * {@code RETRY_WAITING} (a regra de visibilidade) — do {@code Clock}
     * injetado do chamador, como todo "quando" do projeto.
     */
    Optional<Execution> find(ExecutionId id, Instant now);

    /**
     * Página do dashboard — mesma derivação de {@link #find}, sem attempts
     * (sumário por contrato, como na era pré-split); ordenada por id
     * decrescente (UUIDv7 = mais recente primeiro), keyset por
     * {@code id < cursor}. Filtro de {@code status} sobre o estado
     * DERIVADO: terminal filtra na coluna; {@code RUNNING} filtra por
     * posse; {@code ENQUEUED}/{@code RETRY_WAITING} filtram pela fila.
     */
    List<Execution> findPage(@Nullable JobKey jobKey, @Nullable ExecutionState status, @Nullable Instant from,
            @Nullable Instant to, @Nullable ExecutionId cursor, int limit, Instant now);

    /**
     * Contagem por estado do trabalho vivo pro {@code GET /overview} —
     * custo proporcional ao trabalho vivo por construção na mesa nova:
     * {@code RUNNING} = tamanho de {@code mohs_lease}; {@code ENQUEUED}/
     * {@code RETRY_WAITING} = {@code mohs_ready} partida pela regra de
     * visibilidade. Nenhuma leitura toca a história.
     */
    Map<ExecutionState, Long> countActiveByState(Instant now);

    /** Vazão da janela recente ({@code GET /overview}): attempts terminais com {@code finished_at >= since}, por outcome — o índice de throughput serve por construção. */
    Map<ExecutionState, Long> countTerminalOutcomesSince(Instant since);

    /** Poda de {@code mohs_idempotency} pela janela de idempotência (§7.2) — chamada pelo housekeeping, não pelo caminho quente. */
    int pruneIdempotencyBefore(Instant cutoff);
}
