package io.mohs.store.jdbc;

import java.time.Clock;
import java.time.Instant;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

import javax.sql.DataSource;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import io.mohs.core.execution.ExecutionId;
import io.mohs.core.job.JobKey;
import io.mohs.engine.BatchStore;
import io.mohs.engine.WorkQueue;
import io.mohs.store.jdbc.dialect.ClaimedReady;
import io.mohs.store.jdbc.dialect.JdbcDialect;

/**
 * {@link WorkQueue} sobre {@code mohs_ready}/{@code mohs_lease} (Phase 5,
 * ADR-A). O claim é a transação do §6.2 — fila e posse mudam juntas ou
 * nada muda; a forma do SQL é do {@link JdbcDialect} (statement único no
 * Postgres, três statements portáteis nos demais). {@link #offer} NÃO
 * abre transação de propósito (§7.5-1): o enqueue participa da transação
 * do chamador — é o "joins your transaction" da ADR-0003 §4, agora com
 * execução + fila + idempotência na mesma unidade.
 */
public final class JdbcWorkQueue implements WorkQueue {

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final TransactionTemplate claimTransaction;
    private final JdbcDialect dialect;
    private final BatchStore batchStore;
    private final Clock clock;

    public JdbcWorkQueue(DataSource dataSource, JdbcDialect dialect, BatchStore batchStore, Clock clock) {
        Objects.requireNonNull(dataSource, "dataSource");
        this.batchStore = Objects.requireNonNull(batchStore, "batchStore");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.jdbcTemplate = new NamedParameterJdbcTemplate(dataSource);
        this.claimTransaction = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
        // §7.5: claim é transação PRÓPRIA, sempre — REQUIRES_NEW torna isso
        // executável em vez de convencionado: com o REQUIRED default, uma
        // transação externa (interceptor, teste) herdaria a isolação DELA e
        // o READ COMMITTED abaixo seria ignorado em silêncio (MySQL = RR,
        // a divergência que a DBTUNE-4 matou). O engine chama do próprio
        // loop, sem transação externa — a suspensão nunca acontece em
        // operação normal.
        this.claimTransaction.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        // mesmo raciocínio da DBTUNE-4: SKIP LOCKED + inserts assumem
        // READ COMMITTED explícito, nunca o default do banco (MySQL = RR).
        this.claimTransaction.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
        this.dialect = Objects.requireNonNull(dialect, "dialect");
    }

    @Override
    public List<ClaimedWork> claim(int shard, String nodeId, long epoch, int limit, Collection<JobKey> inadmissible,
            Instant now) {
        Objects.requireNonNull(nodeId, "nodeId");
        Objects.requireNonNull(now, "now");
        if (limit <= 0) {
            return List.of();
        }
        List<String> inadmissibleKeys = inadmissible.stream().map(JobKey::value).toList();
        // requireNonNull documenta o invariante pro @NullMarked (JAVA-8) — o callback nunca devolve null
        List<ClaimedReady> claimed = Objects.requireNonNull(claimTransaction.execute(
                _ -> dialect.claimReady(jdbcTemplate, shard, nodeId, epoch, limit, inadmissibleKeys, now)));
        return claimed.stream()
                .map(row -> new ClaimedWork(ExecutionId.of(row.executionId()), JobKey.of(row.jobKey()), row.attempt(), row.priority()))
                .toList();
    }

    @Override
    public void offer(List<ReadyEntry> entries) {
        if (entries.isEmpty()) {
            return;
        }
        jdbcTemplate.batchUpdate(JdbcSupport.READY_INSERT, entries.stream()
                .map(entry -> JdbcSupport.readyEntryParams(entry, dialect))
                .toArray(MapSqlParameterSource[]::new));
        notifyShardsWithDueEntries(entries);
    }

    /**
     * Tier 2 do wake-up (§5.5): sinaliza os shards com entrada já DEVIDA —
     * no-op fora do Postgres; entra na transação do chamador, então o
     * sinal só existe se o INSERT existir. Futuro fica de fora: acordar
     * um nó pra uma linha invisível seria um lap perdido (retries nunca
     * passam por aqui — renascem na conclusão cercada do LeaseStore).
     */
    private void notifyShardsWithDueEntries(List<ReadyEntry> entries) {
        Instant now = clock.instant();
        List<Integer> dueShards = entries.stream()
                .filter(entry -> !entry.visibleAt().isAfter(now))
                .map(ReadyEntry::shard)
                .distinct()
                .toList();
        if (!dueShards.isEmpty()) {
            dialect.notifyReady(jdbcTemplate, dueShards);
        }
    }

    @Override
    public int requeue(List<Requeue> orders) {
        if (orders.isEmpty()) {
            return 0;
        }
        // mesma ordem canônica dos DELETEs do complete (JCIP cap. 10 em row
        // locks): requeue e conclusão travam conjuntos sobrepostos — em ordens
        // opostas seria o AB-BA que o bench do S5.5 mediu (23 deadlocks)
        List<Requeue> ordered = orders.stream()
                .sorted(Comparator.comparing(order -> order.executionId().value()))
                .toList();
        // requireNonNull: mesmo invariante de claim()
        return Objects.requireNonNull(claimTransaction.execute(_ -> {
            int requeued = 0;
            for (Requeue order : ordered) {
                int fenceWon = jdbcTemplate.update(JdbcSupport.FENCED_LEASE_DELETE,
                        JdbcSupport.fencedLeaseDeleteParams(order.executionId().value(), order.nodeId(), order.epoch()));
                if (fenceWon == 1) {
                    jdbcTemplate.update(JdbcSupport.READY_INSERT, JdbcSupport.readyEntryParams(order.entry(), dialect));
                    requeued++;
                }
            }
            return requeued;
        }));
    }

    @Override
    public boolean cancelQueued(ExecutionId id, Instant now) {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(now, "now");
        // requireNonNull: mesmo invariante de claim()
        return Objects.requireNonNull(claimTransaction.execute(_ -> {
            MapSqlParameterSource idParam = new MapSqlParameterSource("executionId", id.value());
            if (jdbcTemplate.update("DELETE FROM mohs_ready WHERE execution_id = :executionId", idParam) == 0) {
                return false;
            }
            // terminal advisory sem poda de partição — caminho frio, por id
            // (mesmo racional do TERMINAL_UPDATE_UNPRUNED do LeaseStore)
            // batch-counted: incrementFailed logo abaixo, nesta transação
            jdbcTemplate.update("""
                    UPDATE mohs_execution SET state = 'CANCELLED', finished_at = :finishedAt
                    WHERE execution_id = :executionId
                    """, idParam.addValue("finishedAt", dialect.splitTimestamp(now)));
            // ADR-0043: cancelar é terminal e um fim que não conta deixa o lote
            // aberto pra sempre — conta como falha na MESMA transação do delete
            // (sem o delete ter pego a entrada, não se chega aqui = conta uma vez)
            String batchId = jdbcTemplate.queryForObject(
                    "SELECT correlation_id FROM mohs_execution WHERE execution_id = :executionId", idParam, String.class);
            if (batchId != null) {
                batchStore.incrementFailed(batchId);
            }
            return true;
        }));
    }

    /**
     * O CAS e o renascimento na fila num único par guardado: o UPDATE só
     * vence com o advisory {@code FAILED} e o job vivo (EXISTS estreita a
     * janela contra um {@code remove} concorrente — mesma semântica do CAS
     * da era anterior); o INSERT deriva attempt e prioridade da própria
     * história ({@code attempts gravados + 1}; a prioridade original), sem
     * o chamador carregar nada.
     */
    @Override
    public boolean rearmForManualRetry(ExecutionId id, Instant now) {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(now, "now");
        // requireNonNull: mesmo invariante de claim()
        return Objects.requireNonNull(claimTransaction.execute(_ -> {
            MapSqlParameterSource params = new MapSqlParameterSource("executionId", id.value());
            // correlation_id IS NULL: membro de lote não rearma (ADR-0043) — o
            // lote já contou esta falha; re-rodar contaria o desfecho DUAS vezes
            // num lote possivelmente já fechado (pending negativo, segundo
            // BatchCompleted). Mesmo guard do CAS da era anterior.
            int rearmed = jdbcTemplate.update("""
                    UPDATE mohs_execution SET state = 'PENDING', finished_at = NULL
                    WHERE execution_id = :executionId AND state = 'FAILED'
                      AND correlation_id IS NULL
                      AND EXISTS (SELECT 1 FROM mohs_job_definitions j
                                  WHERE j.job_key = mohs_execution.job_key AND j.retired = :retired)
                    """, params.addValue("retired", false));
            if (rearmed == 0) {
                return false;
            }
            jdbcTemplate.update("""
                    INSERT INTO mohs_ready (execution_id, job_key, shard, priority, attempt, visible_at)
                    SELECT e.execution_id, e.job_key, e.shard, e.priority,
                           (SELECT COUNT(*) + 1 FROM mohs_attempt a WHERE a.execution_id = e.execution_id),
                           :visibleAt
                    FROM mohs_execution e WHERE e.execution_id = :executionId
                    """, params.addValue("visibleAt", dialect.splitTimestamp(now)));
            return true;
        }));
    }
}
