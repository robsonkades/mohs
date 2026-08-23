package io.mohs.store.jdbc;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

import javax.sql.DataSource;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import io.mohs.core.execution.ExecutionId;
import io.mohs.core.job.JobKey;
import io.mohs.engine.BatchStore;
import io.mohs.engine.Shards;
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

    /**
     * Janela de conflação do NOTIFY (P1): no máximo UM commit notificante
     * por janela POR EMISSOR (~20/s), muito abaixo do teto de serialização
     * medido (~500 notificantes/s nesta máquina — transação notificante
     * não participa de group commit). A primeira versão da P1 (janela por
     * shard) ainda deixava ~52% dos commits notificantes num burst
     * uniforme (4/s × 64 shards) e o wall do ingest ficou 1,55× o
     * baseline — a janela global é o que devolve o group commit de
     * verdade. Constante, não knob (CLAUDE.md): só vira configuração se
     * uma medição mandar.
     */
    private static final long NOTIFY_CONFLATION_WINDOW_NANOS = 50_000_000L;

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final TransactionTemplate claimTransaction;
    private final JdbcDialect dialect;
    private final BatchStore batchStore;
    private final Clock clock;
    /** Shards devidos ainda NÃO sinalizados (bitmask, {@link Shards#maskOf}-compatível) — quem vencer a próxima janela carrega tudo que acumulou aqui. */
    private final AtomicLong pendingNotifyShards = new AtomicLong();
    /** Último NOTIFY em tempo MONOTÔNICO ({@code System.nanoTime} — janela é duração, invariante CLAUDE.md); semeado com "janela já vencida" porque a origem do nanoTime é arbitrária (zero literal suprimiria o primeiro sinal em JVM cujo nanoTime corre negativo). */
    private final AtomicLong lastNotifyNanos = new AtomicLong(System.nanoTime() - NOTIFY_CONFLATION_WINDOW_NANOS);

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
     *
     * <p>CONFLAÇÃO global por emissor (P1 do S6.3, medida): transação
     * notificante não participa de group commit — o Postgres segura um
     * lock GLOBAL do notify queue até o fim do flush do commit
     * ({@code PreCommit_Notify}), e notificar todo enqueue serializou o
     * ingest REST fim a fim de ~680 pra ~345 req/s (latência 7,7ms →
     * 1,3s; A/B de binários na mesma sessão, BASELINE Phase 6). Todo
     * offer ACUMULA seus shards devidos em {@link #pendingNotifyShards};
     * só quem vence a janela ({@link #NOTIFY_CONFLATION_WINDOW_NANOS})
     * emite — e emite TUDO que acumulou, no mesmo statement. O dono de um
     * shard suprimido ou já está no piso do poll (o wake anterior o
     * acordou e o backoff zera a cada tick com trabalho) ou recebe o
     * sinal na próxima janela; a cauda de um burst sem tráfego seguinte
     * fica pro poll — o mesmo best-effort já contratado (rollback do
     * vencedor idem: os bits já saíram da máscara e o poll cobre; e o
     * carry pode SAIR ANTES de o INSERT concorrente que o gerou commitar
     * — wake adiantado, lap vazio, a linha fica pro poll; validado ao
     * vivo no review da P1).
     */
    private void notifyShardsWithDueEntries(List<ReadyEntry> entries) {
        long dueMask = dueShardMask(entries);
        if (dueMask == 0L) {
            return;
        }
        pendingNotifyShards.getAndAccumulate(dueMask, (pending, add) -> pending | add);
        if (!winsNotifyWindow()) {
            return; // janela fechada, ou outro enqueue concorrente venceu — os bits ficam pra ele/próxima
        }
        long toNotify = pendingNotifyShards.getAndSet(0L);
        if (toNotify != 0L) {
            dialect.notifyReady(jdbcTemplate, shardsOf(toNotify));
        }
    }

    /** Bitmask ({@link Shards#maskOf}-compatível) dos shards com entrada já devida — entrada futura fica de fora por design (ver {@link #notifyShardsWithDueEntries}). */
    private long dueShardMask(List<ReadyEntry> entries) {
        Instant now = clock.instant();
        long dueMask = 0L;
        for (ReadyEntry entry : entries) {
            if (!entry.visibleAt().isAfter(now)) {
                dueMask |= 1L << entry.shard();
            }
        }
        return dueMask;
    }

    /** Disputa a janela global de conflação: só UM emissor vence por {@link #NOTIFY_CONFLATION_WINDOW_NANOS} — perder o CAS significa que outro enqueue concorrente acabou de vencer e carrega os bits pendentes. */
    private boolean winsNotifyWindow() {
        long nowNanos = System.nanoTime();
        long last = lastNotifyNanos.get();
        return nowNanos - last >= NOTIFY_CONFLATION_WINDOW_NANOS
                && lastNotifyNanos.compareAndSet(last, nowNanos);
    }

    private static List<Integer> shardsOf(long mask) {
        List<Integer> shards = new ArrayList<>(Long.bitCount(mask));
        for (int shard = 0; shard < Shards.SHARD_COUNT; shard++) {
            if ((mask & (1L << shard)) != 0) {
                shards.add(shard);
            }
        }
        return shards;
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
