package io.mohs.store.jdbc;

import java.time.Instant;
import java.util.Collection;
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

    public JdbcWorkQueue(DataSource dataSource, JdbcDialect dialect) {
        Objects.requireNonNull(dataSource, "dataSource");
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
                .map(row -> new ClaimedWork(ExecutionId.of(row.executionId()), JobKey.of(row.jobKey()), row.attempt()))
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
    }

    @Override
    public int requeue(List<Requeue> orders) {
        if (orders.isEmpty()) {
            return 0;
        }
        // requireNonNull: mesmo invariante de claim()
        return Objects.requireNonNull(claimTransaction.execute(_ -> {
            int requeued = 0;
            for (Requeue order : orders) {
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
}
