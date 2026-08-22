package io.mohs.engine;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.jspecify.annotations.Nullable;

import io.mohs.core.execution.Attempt;
import io.mohs.core.execution.ExecutionId;
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
     * chave de partição dos ids reivindicados. Infra e linha separadas
     * como na ADR-0047: falha de deserialização de UMA linha não derruba o
     * lote — a linha entra em {@code failed} e o resto despacha.
     */
    PayloadBatch findPayloads(List<ExecutionId> ids);

    /** Payload hidratado + o {@code created_at} que a conclusão vai precisar pra podar a partição do UPDATE terminal. */
    record PayloadRow(ExecutionId executionId, Instant createdAt, Object payload) {
        public PayloadRow {
            Objects.requireNonNull(executionId, "executionId");
            Objects.requireNonNull(createdAt, "createdAt");
            Objects.requireNonNull(payload, "payload");
        }
    }

    /** Mesma separação de {@code ExecutionStore.PayloadBatch} (ADR-0047): {@code unreadable} = linhas ilegíveis (falha PERMANENTE, com a causa pro attempt terminal), nunca infra — infra propaga como exceção da própria chamada. */
    record PayloadBatch(Map<ExecutionId, PayloadRow> rows, Map<ExecutionId, RuntimeException> unreadable) {
        public PayloadBatch {
            rows = Map.copyOf(rows);
            unreadable = Map.copyOf(unreadable);
        }
    }

    /** Attempts de uma execução, em ordem de número — o detail view (servido por {@code idx_mohs_attempt_exec} no PG). */
    List<Attempt> findAttempts(ExecutionId executionId);

    /** Poda de {@code mohs_idempotency} pela janela de idempotência (§7.2) — chamada pelo housekeeping, não pelo caminho quente. */
    int pruneIdempotencyBefore(Instant cutoff);
}
