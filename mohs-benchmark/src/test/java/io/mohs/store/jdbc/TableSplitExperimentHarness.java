package io.mohs.store.jdbc;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.sql.DataSource;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import tools.jackson.databind.json.JsonMapper;

import io.github.robsonkades.uuidv7.UUIDv7;
import io.mohs.core.definition.JobDefinition;
import io.mohs.core.execution.Attempt;
import io.mohs.core.execution.Execution;
import io.mohs.core.execution.ExecutionId;
import io.mohs.core.execution.ExecutionState;
import io.mohs.core.execution.Priority;
import io.mohs.core.job.JobKey;
import io.mohs.engine.ExecutionStore;
import io.mohs.engine.ExecutionWindowRegistry;
import io.mohs.store.jdbc.dialect.PostgresJdbcDialect;
import io.mohs.test.MutableClock;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Experimento E1 do ARCHITECTURE_REDESIGN_PLAN (§20.3), Postgres (Tier 1)
 * — decide a ADR-A (o split de tabelas do §7) ANTES da Phase 5: replay do
 * MESMO workload de 500k execuções fim a fim (enqueue → claim → conclusão)
 * nos dois schemas, comparando físicas de escrita, não formas de query
 * (isso foi o E2).
 *
 * <p>Três braços, mesmo container e mesma estrutura de concorrência
 * (8 claimers, dispatch limitado por semáforo):
 * <ul>
 *   <li><b>current</b> — o caminho real de hoje: {@link JdbcExecutionStore}
 *       ({@code insert}/{@code markFired}/{@code complete}) +
 *       {@link JdbcClaimer}, sobre {@code schema-postgresql.sql};</li>
 *   <li><b>split-sync</b> — o schema §7.2 ({@code e1_ready}/{@code e1_lease}/
 *       {@code e1_execution}/{@code e1_attempt} particionadas), claim
 *       round-robin single-shard (a forma que venceu o E2, leitura batched de
 *       payload incluída), conclusão em transação própria por execução —
 *       isola o ganho da ADR-A sem o group commit;</li>
 *   <li><b>split-group</b> — idem + o {@code CompletionBatcher} do §7.6
 *       (flush de 256 via {@code unnest}) — o alvo completo do plano; é
 *       sobre este braço que os kill criteria do E1 são avaliados.</li>
 * </ul>
 *
 * <p>Kill criteria (§20.3): vazão de drain &lt; 1,5× a atual, ou WAL bytes
 * por execução (ciclo completo) não caindo ≥ 40%. Também mede o claim
 * falsificável do §3.3: tuple versions na tabela grande &gt; 3/execução
 * falsifica o split (alvo: 2 = insert + update terminal).
 *
 * <p>Vieses declarados, ambos contra o braço novo: o braço current NÃO paga
 * a cerimônia de leitura do Dispatcher real (~1,9 round trips autocommit
 * por execução, medida na Phase 0) — só o que {@code claim()} já faz — e
 * roda com pool 100/dispatch 256, abaixo do ponto de operação (300/1024).
 * Checkpoints são afastados do run ({@code max_wal_size} via ALTER SYSTEM)
 * e o FPI restante é reportado por fase para o leitor ver a contaminação.
 * Roda só por {@code -Dtest=} explícito, como todo harness.
 */
class TableSplitExperimentHarness {

    private static final Instant NOW = Instant.parse("2026-08-21T12:00:00Z");
    private static final Duration LEASE_TTL = Duration.ofSeconds(30);

    private static final int EXECUTIONS = 500_000;
    private static final int WARMUP_EXECUTIONS = 20_000;
    private static final int JOB_COUNT = 100;
    private static final int CLAIMERS = 8;
    private static final int CLAIM_BATCH = 100;
    private static final int SHARD_COUNT = 64;
    private static final int DISPATCH_PERMITS = 256;
    private static final int GROUP_FLUSH = 256;
    private static final int REPEATS = 3;
    private static final int POOL_SIZE = 100;

    private enum Arm {
        CURRENT("current"), SPLIT_SYNC("split-sync"), SPLIT_GROUP("split-group");

        final String label;

        Arm(String label) {
            this.label = label;
        }
    }

    record Handler() {
    }

    /**
     * ~500 B de payload, não {@code '{}'}: o Postgres loga a tupla INTEIRA em
     * cada versão nova, então payload trivial é o caso adversarial para a
     * métrica de WAL do E1 — as ~3 versões/execução do braço atual crescem
     * linearmente com o payload, a ~1 do split não (rodada 1, registrada no
     * BASELINE, mediu com '{}' e ficou em −31,6%).
     */
    record Payload(String data) {
    }

    private static final Payload PAYLOAD = new Payload("x".repeat(480));
    private static final String PAYLOAD_JSON = "{\"data\":\"" + PAYLOAD.data() + "\"}";
    private static final String PAYLOAD_TYPE = Payload.class.getName();
    private static final JsonMapper PAYLOAD_MAPPER = JsonMapper.builder().build();

    // ── SQL do schema-alvo (§7.2, recorte do workload: sem idempotency/trigger) ──

    private static final String SPLIT_CLAIM = """
            WITH picked AS (
                SELECT execution_id, job_key, attempt
                  FROM e1_ready
                 WHERE shard = ?
                   AND visible_at <= ?
                   AND job_key <> ALL('{}'::text[])
                 ORDER BY priority, visible_at
                 LIMIT ?
                   FOR UPDATE SKIP LOCKED
            ),
            gone AS (
                DELETE FROM e1_ready r USING picked p
                 WHERE r.execution_id = p.execution_id
             RETURNING r.execution_id, r.job_key, r.attempt
            )
            INSERT INTO e1_lease (execution_id, job_key, node_id, epoch, attempt_number, claimed_at)
            SELECT execution_id, job_key, ?, 1, attempt, ? FROM gone
            RETURNING execution_id
            """;

    private static final String SPLIT_PAYLOAD = """
            SELECT execution_id, created_at, payload, payload_type FROM e1_execution
             WHERE execution_id = ANY(?) AND created_at >= ? AND created_at < ?
            """;

    private static final String SPLIT_ENQUEUE_EXECUTION = """
            INSERT INTO e1_execution (
                execution_id, job_key, tenant_id, shard, priority, state, scheduled_at, created_at,
                actor, payload, payload_type)
            VALUES (?, ?, '', ?, ?, 'PENDING', ?, ?, 'harness', ?::jsonb, ?)
            """;

    private static final String SPLIT_ENQUEUE_READY = """
            INSERT INTO e1_ready (execution_id, job_key, shard, priority, attempt, visible_at, tenant_id)
            VALUES (?, ?, ?, ?, 1, ?, '')
            """;

    private static final String SYNC_COMPLETE_LEASE =
            "DELETE FROM e1_lease WHERE execution_id = ? AND node_id = ? AND epoch = 1";
    private static final String SYNC_COMPLETE_ATTEMPT = """
            INSERT INTO e1_attempt (execution_id, number, node_id, started_at, finished_at, outcome, error_type, error)
            VALUES (?, 1, ?, ?, ?, 'SUCCEEDED', NULL, NULL)
            """;
    private static final String SYNC_COMPLETE_EXECUTION =
            "UPDATE e1_execution SET state = 'SUCCEEDED', finished_at = ? WHERE execution_id = ? AND created_at = ?";

    private static final String GROUP_COMPLETE_LEASE = """
            DELETE FROM e1_lease l USING unnest(?::uuid[]) AS x(id)
             WHERE l.execution_id = x.id AND l.node_id = ? AND l.epoch = 1
            RETURNING l.execution_id
            """;
    private static final String GROUP_COMPLETE_ATTEMPT = """
            INSERT INTO e1_attempt (execution_id, number, node_id, started_at, finished_at, outcome, error_type, error)
            SELECT x.id, 1, ?, ?, ?, 'SUCCEEDED', NULL, NULL FROM unnest(?::uuid[]) AS x(id)
            """;
    private static final String GROUP_COMPLETE_EXECUTION = """
            UPDATE e1_execution e SET state = 'SUCCEEDED', finished_at = ?
              FROM unnest(?::uuid[], ?::timestamptz[]) AS x(id, created_at)
             WHERE e.execution_id = x.id AND e.created_at = x.created_at
            """;

    // ── medição ─────────────────────────────────────────────────────────────

    /**
     * {@code pg_stat} lag: o coletor descarrega por backend em até ~1s, mas o
     * sono fixo de 1,2s do E2 não bastou aqui — no drain de 3,3s do braço
     * group da rodada 1, ~6% dos contadores de tabela ainda não tinham
     * aterrissado no snapshot. Depois do sono, relê até dois snapshots
     * consecutivos coincidirem em WAL + contadores (dead_tup fica fora da
     * convergência: é gauge e o autovacuum o move continuamente), com teto —
     * se não convergir, registra o último e o relatório carrega o resíduo.
     */
    record StatSnapshot(long walBytes, long walRecords, long walFpi, Map<String, TableStat> tables) {

        private static final int STABILIZE_MAX_READS = 25;

        static StatSnapshot take(JdbcTemplate jdbc) throws InterruptedException {
            Thread.sleep(1200);
            StatSnapshot previous = read(jdbc);
            for (int i = 0; i < STABILIZE_MAX_READS; i++) {
                Thread.sleep(600);
                StatSnapshot next = read(jdbc);
                if (next.countersEqual(previous)) {
                    return next;
                }
                previous = next;
            }
            System.err.println("stat snapshot did not converge after " + STABILIZE_MAX_READS
                    + " reads — o delta desta fase carrega resíduo de flush");
            return previous;
        }

        private boolean countersEqual(StatSnapshot other) {
            if (walBytes != other.walBytes || walRecords != other.walRecords || walFpi != other.walFpi
                    || !tables.keySet().equals(other.tables.keySet())) {
                return false;
            }
            return tables.entrySet().stream()
                    .allMatch(e -> e.getValue().writeCountersEqual(other.tables.get(e.getKey())));
        }

        private static StatSnapshot read(JdbcTemplate jdbc) {
            Map<String, Object> wal = jdbc.queryForMap("SELECT wal_bytes, wal_records, wal_fpi FROM pg_stat_wal");
            Map<String, TableStat> tables = new LinkedHashMap<>();
            jdbc.query("""
                    SELECT relname, n_tup_ins, n_tup_upd, n_tup_hot_upd, n_tup_del, n_dead_tup
                    FROM pg_stat_user_tables
                    """, rs -> {
                tables.put(rs.getString("relname"), new TableStat(rs.getLong("n_tup_ins"), rs.getLong("n_tup_upd"),
                        rs.getLong("n_tup_hot_upd"), rs.getLong("n_tup_del"), rs.getLong("n_dead_tup")));
            });
            return new StatSnapshot(((Number) wal.get("wal_bytes")).longValue(),
                    ((Number) wal.get("wal_records")).longValue(),
                    ((Number) wal.get("wal_fpi")).longValue(), tables);
        }
    }

    record TableStat(long ins, long upd, long hotUpd, long del, long deadTup) {

        static final TableStat ZERO = new TableStat(0, 0, 0, 0, 0);

        TableStat plus(TableStat other) {
            return new TableStat(ins + other.ins, upd + other.upd, hotUpd + other.hotUpd,
                    del + other.del, deadTup + other.deadTup);
        }

        TableStat minus(TableStat other) {
            return new TableStat(ins - other.ins, upd - other.upd, hotUpd - other.hotUpd,
                    del - other.del, deadTup - other.deadTup);
        }

        boolean hasWrites() {
            return ins != 0 || upd != 0 || del != 0;
        }

        /** Só os contadores monotônicos de escrita — {@code deadTup} é gauge e fica fora da convergência. */
        boolean writeCountersEqual(TableStat other) {
            return ins == other.ins && upd == other.upd && hotUpd == other.hotUpd && del == other.del;
        }
    }

    record PhaseResult(double seconds, long walBytes, long walRecords, long walFpi,
            Map<String, TableStat> tableDeltas) {

        static PhaseResult of(double seconds, StatSnapshot before, StatSnapshot after) {
            Map<String, TableStat> deltas = new LinkedHashMap<>();
            after.tables().forEach((name, stat) ->
                    deltas.put(name, stat.minus(before.tables().getOrDefault(name, TableStat.ZERO))));
            return new PhaseResult(seconds, after.walBytes() - before.walBytes(),
                    after.walRecords() - before.walRecords(), after.walFpi() - before.walFpi(), deltas);
        }

        TableStat sumFor(String prefix) {
            return tableDeltas.entrySet().stream()
                    .filter(e -> e.getKey().startsWith(prefix))
                    .map(Map.Entry::getValue)
                    .reduce(TableStat.ZERO, TableStat::plus);
        }
    }

    record RunResult(Arm arm, int executions, PhaseResult enqueue, PhaseResult drain) {

        double enqueueRate() {
            return executions / enqueue.seconds();
        }

        double drainRate() {
            return executions / drain.seconds();
        }

        double lifecycleWalPerExec() {
            return (enqueue.walBytes() + drain.walBytes()) / (double) executions;
        }

        /** ins+upd na(s) tabela(s) grande(s) — o claim falsificável do §3.3. */
        double bigTableVersionsPerExec() {
            String prefix = arm == Arm.CURRENT ? "mohs_executions" : "e1_execution_";
            TableStat lifecycle = enqueue.sumFor(prefix).plus(drain.sumFor(prefix));
            return (lifecycle.ins() + lifecycle.upd()) / (double) executions;
        }
    }

    @Test
    void run() throws Exception {
        raiseWalCeiling();

        // aquecimento descartado — a rodada 1 do E2 mediu p99 de 238ms puro
        // C2/buffers frios; mesma lição aqui
        for (Arm arm : Arm.values()) {
            replay(arm, WARMUP_EXECUTIONS);
        }

        List<RunResult> all = new ArrayList<>();
        List<RunResult> medians = new ArrayList<>();
        for (Arm arm : Arm.values()) {
            List<RunResult> runs = new ArrayList<>(REPEATS);
            for (int i = 0; i < REPEATS; i++) {
                runs.add(replay(arm, EXECUTIONS));
            }
            all.addAll(runs);
            runs.sort(Comparator.comparingDouble(RunResult::drainRate));
            medians.add(runs.get(runs.size() / 2));
        }
        printReport(all, medians);
    }

    /**
     * Sem isto o volume de WAL do replay (~1-1,5 GB no braço atual) estoura o
     * {@code max_wal_size} default (1 GB) e dispara checkpoint NO MEIO da
     * janela medida — FPI contamina o WAL/execução, que é o entregável do E1
     * (a limitação declarada do E2). Ambos os parâmetros são sighup-context;
     * o CHECKPOINT manual no início de cada replay zera o débito acumulado
     * fora da janela — e é só no início de propósito: um checkpoint ENTRE as
     * fases faria o drain repagar FPI das páginas que o enqueue acabou de
     * escrever.
     */
    private void raiseWalCeiling() {
        JdbcTemplate jdbc = new JdbcTemplate(PostgresTestSupport.freshSchema());
        jdbc.execute("ALTER SYSTEM SET max_wal_size = '16GB'");
        jdbc.execute("ALTER SYSTEM SET checkpoint_timeout = '30min'");
        jdbc.execute("SELECT pg_reload_conf()");
    }

    private RunResult replay(Arm arm, int executions) throws Exception {
        return switch (arm) {
            case CURRENT -> replayCurrent(executions);
            case SPLIT_SYNC -> replaySplit(Arm.SPLIT_SYNC, executions);
            case SPLIT_GROUP -> replaySplit(Arm.SPLIT_GROUP, executions);
        };
    }

    // ── braço A: o caminho de hoje, pelas classes reais ─────────────────────

    private RunResult replayCurrent(int executions) throws Exception {
        DataSource raw = PostgresTestSupport.freshSchema();
        try (HikariDataSource pool = pooledDataSource(raw)) {
            MutableClock clock = new MutableClock(NOW, ZoneId.of("UTC"));
            JdbcTemplate jdbc = new JdbcTemplate(pool);
            JdbcJobStore jobStore = new JdbcJobStore(pool, clock);
            JdbcExecutionStore executionStore = new JdbcExecutionStore(pool, clock, JsonMapper.builder().build(),
                    new PostgresJdbcDialect());
            for (int j = 0; j < JOB_COUNT; j++) {
                jobStore.upsert(JobDefinition.of("e1-job-" + j, Handler.class, spec -> spec.onDemand()));
            }

            jdbc.execute("CHECKPOINT");
            StatSnapshot start = settleAndSnapshot(pool, jdbc);

            long enqueueStart = System.nanoTime();
            runWorkers(CLAIMERS, worker -> {
                for (int i = worker; i < executions; i += CLAIMERS) {
                    String jobKey = "e1-job-" + (i % JOB_COUNT);
                    executionStore.insert(new Execution(ExecutionId.of(UUIDv7.randomUUID().toString()),
                            JobKey.of(jobKey), ExecutionState.ENQUEUED, NOW.minusSeconds(1), null,
                            List.of(), "harness"), PAYLOAD);
                }
            });
            double enqueueSeconds = (System.nanoTime() - enqueueStart) / 1_000_000_000.0;
            StatSnapshot afterEnqueue = settleAndSnapshot(pool, jdbc);
            jdbc.execute("ANALYZE mohs_executions, mohs_job_definitions");

            AtomicInteger completed = new AtomicInteger();
            Semaphore inFlight = new Semaphore(DISPATCH_PERMITS);
            Queue<Future<?>> dispatched = new ConcurrentLinkedQueue<>();
            long drainStart = System.nanoTime();
            try (ExecutorService dispatcher = Executors.newVirtualThreadPerTaskExecutor()) {
                runWorkers(CLAIMERS, worker -> {
                    JdbcClaimer claimer = new JdbcClaimer(pool, new PostgresJdbcDialect(), clock, executionStore,
                            jobStore, LEASE_TTL, new ExecutionWindowRegistry(List.of()),
                            new JdbcRateLimitStore(pool, clock));
                    String nodeId = "current-node-" + worker;
                    List<Execution> claimed;
                    do {
                        claimed = claimer.claim(nodeId, CLAIM_BATCH);
                        for (Execution execution : claimed) {
                            inFlight.acquire();
                            dispatched.add(dispatcher.submit(() -> {
                                try {
                                    // era executionStore.markFired antes da Phase 3 (ADR-0047
                                    // fundiu fired_at no claim); inline pra este braço seguir
                                    // medindo o caminho de escrita que o E1 registrou
                                    new JdbcTemplate(pool).update(
                                            "UPDATE mohs_executions SET fired_at = ? WHERE id = ?",
                                            Timestamp.from(NOW), execution.id().value());
                                    ExecutionStore.Completion completion = executionStore.complete(
                                            new ExecutionStore.CompletionRequest(execution.id(), execution.jobKey(),
                                                    new Attempt(1, NOW, NOW, ExecutionState.SUCCEEDED, null),
                                                    ExecutionState.SUCCEEDED),
                                            jobStore);
                                    if (completion != ExecutionStore.Completion.NOT_APPLIED) {
                                        completed.incrementAndGet();
                                    }
                                } finally {
                                    inFlight.release();
                                }
                                return null;
                            }));
                        }
                    } while (!claimed.isEmpty());
                });
                awaitAll(dispatched);
            }
            double drainSeconds = (System.nanoTime() - drainStart) / 1_000_000_000.0;
            StatSnapshot end = settleAndSnapshot(pool, jdbc);
            assertThat(completed.get()).isEqualTo(executions);
            return new RunResult(Arm.CURRENT, executions,
                    PhaseResult.of(enqueueSeconds, start, afterEnqueue),
                    PhaseResult.of(drainSeconds, afterEnqueue, end));
        }
    }

    // ── braços B/C: o schema §7.2, transações do §7.5 (+ §7.6 no group) ─────

    private RunResult replaySplit(Arm arm, int executions) throws Exception {
        DataSource raw = PostgresTestSupport.freshSchema();
        try (HikariDataSource pool = pooledDataSource(raw)) {
            JdbcTemplate jdbc = new JdbcTemplate(pool);
            createSplitSchema(jdbc);

            jdbc.execute("CHECKPOINT");
            StatSnapshot start = settleAndSnapshot(pool, jdbc);

            long enqueueStart = System.nanoTime();
            runWorkers(CLAIMERS, worker -> enqueueSplitSlice(pool, worker, executions));
            double enqueueSeconds = (System.nanoTime() - enqueueStart) / 1_000_000_000.0;
            StatSnapshot afterEnqueue = settleAndSnapshot(pool, jdbc);
            jdbc.execute("ANALYZE e1_ready, e1_execution");

            AtomicInteger completed = new AtomicInteger();
            long drainStart = System.nanoTime();
            try (ExecutorService dispatcher = Executors.newVirtualThreadPerTaskExecutor()) {
                Semaphore inFlight = new Semaphore(DISPATCH_PERMITS);
                Queue<Future<?>> dispatched = new ConcurrentLinkedQueue<>();
                runWorkers(CLAIMERS, worker -> {
                    String nodeId = "split-node-" + worker;
                    List<Integer> owned = ownedShards(worker);
                    if (arm == Arm.SPLIT_GROUP) {
                        drainGrouped(pool, owned, nodeId, completed);
                    } else {
                        drainSync(pool, owned, nodeId, dispatcher, inFlight, dispatched, completed);
                    }
                });
                awaitAll(dispatched);
            }
            double drainSeconds = (System.nanoTime() - drainStart) / 1_000_000_000.0;
            StatSnapshot end = settleAndSnapshot(pool, jdbc);
            assertThat(completed.get()).isEqualTo(executions);
            // sem isto as e1_* sobreviveriam com tuplas mortas e autovacuum
            // agressivo, que poderia disparar DENTRO da janela medida de um
            // run do braço current e cobrar WAL alheio
            jdbc.execute("DROP TABLE IF EXISTS e1_ready, e1_lease, e1_execution, e1_attempt CASCADE");
            return new RunResult(arm, executions,
                    PhaseResult.of(enqueueSeconds, start, afterEnqueue),
                    PhaseResult.of(drainSeconds, afterEnqueue, end));
        }
    }

    /** A transação de enqueue do §7.5: os dois INSERTs num commit, por execução — a física do ADR-0003 §4 preservada. */
    private void enqueueSplitSlice(DataSource pool, int worker, int executions) throws SQLException {
        Timestamp createdAt = Timestamp.from(NOW);
        Timestamp scheduledAt = Timestamp.from(NOW.minusSeconds(1));
        try (Connection connection = pool.getConnection()) {
            boolean previousAutoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);
            try (PreparedStatement insertExecution = connection.prepareStatement(SPLIT_ENQUEUE_EXECUTION);
                    PreparedStatement insertReady = connection.prepareStatement(SPLIT_ENQUEUE_READY)) {
                for (int i = worker; i < executions; i += CLAIMERS) {
                    UUID id = UUIDv7.randomUUID();
                    String jobKey = "e1-job-" + (i % JOB_COUNT);
                    int shard = Math.floorMod(id.hashCode(), SHARD_COUNT);
                    insertExecution.setObject(1, id);
                    insertExecution.setString(2, jobKey);
                    insertExecution.setInt(3, shard);
                    insertExecution.setInt(4, Priority.NORMAL.value());
                    insertExecution.setTimestamp(5, scheduledAt);
                    insertExecution.setTimestamp(6, createdAt);
                    // serializa POR insert, como o braço current (e o design
                    // real) — bind pré-computado seria viés a favor do split
                    insertExecution.setString(7, PAYLOAD_MAPPER.writeValueAsString(PAYLOAD));
                    insertExecution.setString(8, PAYLOAD_TYPE);
                    insertExecution.executeUpdate();
                    insertReady.setObject(1, id);
                    insertReady.setString(2, jobKey);
                    insertReady.setInt(3, shard);
                    insertReady.setInt(4, Priority.NORMAL.value());
                    insertReady.setTimestamp(5, scheduledAt);
                    insertReady.executeUpdate();
                    connection.commit();
                }
            } finally {
                connection.setAutoCommit(previousAutoCommit);
            }
        }
    }

    private record ClaimedRef(UUID id, Timestamp createdAt) {
    }

    /** Conclusão na transação própria (§7.5 item 3, flush=1) — a ADR-A sozinha, sem o amortecedor do §7.6. */
    private void drainSync(DataSource pool, List<Integer> owned, String nodeId, ExecutorService dispatcher,
            Semaphore inFlight, Queue<Future<?>> dispatched, AtomicInteger completed) throws Exception {
        int cursor = 0;
        int consecutiveEmpty = 0;
        while (consecutiveEmpty < owned.size()) {
            int shard = owned.get(cursor);
            cursor = (cursor + 1) % owned.size();
            List<ClaimedRef> claimed = claimRound(pool, shard, nodeId);
            consecutiveEmpty = claimed.isEmpty() ? consecutiveEmpty + 1 : 0;
            for (ClaimedRef ref : claimed) {
                inFlight.acquire();
                dispatched.add(dispatcher.submit(() -> {
                    try (Connection connection = pool.getConnection()) {
                        connection.setAutoCommit(false);
                        try (PreparedStatement lease = connection.prepareStatement(SYNC_COMPLETE_LEASE);
                                PreparedStatement attempt = connection.prepareStatement(SYNC_COMPLETE_ATTEMPT);
                                PreparedStatement execution = connection.prepareStatement(SYNC_COMPLETE_EXECUTION)) {
                            Timestamp now = Timestamp.from(NOW);
                            lease.setObject(1, ref.id());
                            lease.setString(2, nodeId);
                            assertThat(lease.executeUpdate()).isEqualTo(1);
                            attempt.setObject(1, ref.id());
                            attempt.setString(2, nodeId);
                            attempt.setTimestamp(3, now);
                            attempt.setTimestamp(4, now);
                            assertThat(attempt.executeUpdate()).isEqualTo(1);
                            execution.setTimestamp(1, now);
                            execution.setObject(2, ref.id());
                            execution.setTimestamp(3, ref.createdAt());
                            assertThat(execution.executeUpdate()).isEqualTo(1);
                        }
                        connection.commit();
                        completed.incrementAndGet();
                    } finally {
                        inFlight.release();
                    }
                    return null;
                }));
            }
        }
    }

    /**
     * O {@code CompletionBatcher} do §7.6 reduzido ao replay: handler trivial
     * ⇒ o próprio claimer acumula e descarrega no gatilho de N (o gatilho de
     * T não dispara num drain saturado). Flush dentro do laço de claim é
     * conservador — a latência do flush entra no caminho do claim, o que o
     * design real evita com a fila.
     */
    private void drainGrouped(DataSource pool, List<Integer> owned, String nodeId, AtomicInteger completed)
            throws Exception {
        List<ClaimedRef> buffer = new ArrayList<>(GROUP_FLUSH + CLAIM_BATCH);
        int cursor = 0;
        int consecutiveEmpty = 0;
        while (consecutiveEmpty < owned.size()) {
            int shard = owned.get(cursor);
            cursor = (cursor + 1) % owned.size();
            List<ClaimedRef> claimed = claimRound(pool, shard, nodeId);
            consecutiveEmpty = claimed.isEmpty() ? consecutiveEmpty + 1 : 0;
            buffer.addAll(claimed);
            if (buffer.size() >= GROUP_FLUSH) {
                flushCompletions(pool, nodeId, buffer, completed);
            }
        }
        flushCompletions(pool, nodeId, buffer, completed);
    }

    private void flushCompletions(DataSource pool, String nodeId, List<ClaimedRef> buffer, AtomicInteger completed)
            throws SQLException {
        if (buffer.isEmpty()) {
            return;
        }
        try (Connection connection = pool.getConnection()) {
            connection.setAutoCommit(false);
            Array ids = connection.createArrayOf("uuid", buffer.stream().map(ClaimedRef::id).toArray());
            // Instant ISO-8601 (offset explícito), não Timestamp: o encoder de
            // OBJECT_ARRAY do pgjdbc serializa Timestamp[] via toString() em
            // hora de parede local, e um mismatch de timezone faria o UPDATE
            // da tabela grande casar 0 linhas em silêncio — inflando exatamente
            // o número de WAL que decide o kill criterion (review do E1)
            Array createdAts = connection.createArrayOf("timestamptz",
                    buffer.stream().map(ref -> ref.createdAt().toInstant().toString()).toArray());
            Timestamp now = Timestamp.from(NOW);
            int ownedRows;
            try (PreparedStatement lease = connection.prepareStatement(GROUP_COMPLETE_LEASE)) {
                lease.setArray(1, ids);
                lease.setString(2, nodeId);
                ownedRows = countRows(lease);
            }
            try (PreparedStatement attempt = connection.prepareStatement(GROUP_COMPLETE_ATTEMPT)) {
                attempt.setString(1, nodeId);
                attempt.setTimestamp(2, now);
                attempt.setTimestamp(3, now);
                attempt.setArray(4, ids);
                assertThat(attempt.executeUpdate()).isEqualTo(buffer.size());
            }
            try (PreparedStatement execution = connection.prepareStatement(GROUP_COMPLETE_EXECUTION)) {
                execution.setTimestamp(1, now);
                execution.setArray(2, ids);
                execution.setArray(3, createdAts);
                assertThat(execution.executeUpdate()).isEqualTo(buffer.size());
            }
            connection.commit();
            // o RETURNING do DELETE é a cerca do §7.6: só conta o que este
            // node ainda possuía — no replay sem reaper tem que ser tudo
            assertThat(ownedRows).isEqualTo(buffer.size());
            completed.addAndGet(ownedRows);
        }
        buffer.clear();
    }

    /** Uma rodada da forma vencedora do E2: claim CTE single-shard + a leitura batched de payload (regra do E2: sem ela mede-se uma query que não existe). */
    private List<ClaimedRef> claimRound(DataSource pool, int shard, String nodeId) {
        try (Connection connection = pool.getConnection()) {
            List<UUID> claimed = new ArrayList<>(CLAIM_BATCH);
            try (PreparedStatement ps = connection.prepareStatement(SPLIT_CLAIM)) {
                ps.setInt(1, shard);
                ps.setTimestamp(2, Timestamp.from(NOW));
                ps.setInt(3, CLAIM_BATCH);
                ps.setString(4, nodeId);
                ps.setTimestamp(5, Timestamp.from(NOW));
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        claimed.add(rs.getObject(1, UUID.class));
                    }
                }
            }
            if (claimed.isEmpty()) {
                return List.of();
            }
            List<ClaimedRef> refs = new ArrayList<>(claimed.size());
            try (PreparedStatement ps = connection.prepareStatement(SPLIT_PAYLOAD)) {
                ps.setArray(1, connection.createArrayOf("uuid", claimed.toArray()));
                ps.setTimestamp(2, Timestamp.from(NOW.minus(Duration.ofHours(1))));
                ps.setTimestamp(3, Timestamp.from(NOW.plus(Duration.ofHours(1))));
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        refs.add(new ClaimedRef(rs.getObject(1, UUID.class), rs.getTimestamp(2)));
                    }
                }
            }
            assertThat(refs).hasSameSizeAs(claimed);
            return refs;
        } catch (SQLException e) {
            throw new IllegalStateException("split claim round failed", e);
        }
    }

    private static int countRows(PreparedStatement ps) throws SQLException {
        int rows = 0;
        try (ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                rows++;
            }
        }
        return rows;
    }

    private void createSplitSchema(JdbcTemplate jdbc) {
        jdbc.execute("DROP TABLE IF EXISTS e1_ready, e1_lease, e1_execution, e1_attempt CASCADE");
        jdbc.execute("""
                CREATE TABLE e1_ready (
                    execution_id uuid        NOT NULL PRIMARY KEY,
                    job_key      text        NOT NULL,
                    shard        smallint    NOT NULL,
                    priority     smallint    NOT NULL,
                    attempt      smallint    NOT NULL,
                    visible_at   timestamptz NOT NULL,
                    tenant_id    text        NOT NULL DEFAULT ''
                ) WITH (fillfactor = 70,
                        autovacuum_vacuum_scale_factor = 0.0, autovacuum_vacuum_threshold = 1000,
                        autovacuum_vacuum_cost_delay = 0)
                """);
        jdbc.execute("CREATE INDEX e1_ready_claim ON e1_ready (shard, priority, visible_at)"
                + " INCLUDE (execution_id, job_key, attempt)");
        jdbc.execute("""
                CREATE TABLE e1_lease (
                    execution_id     uuid        NOT NULL PRIMARY KEY,
                    job_key          text        NOT NULL,
                    node_id          text        NOT NULL,
                    epoch            bigint      NOT NULL,
                    attempt_number   smallint    NOT NULL,
                    claimed_at       timestamptz NOT NULL,
                    cancel_requested boolean     NOT NULL DEFAULT FALSE
                ) WITH (fillfactor = 70,
                        autovacuum_vacuum_scale_factor = 0.0, autovacuum_vacuum_threshold = 1000,
                        autovacuum_vacuum_cost_delay = 0)
                """);
        jdbc.execute("CREATE INDEX e1_lease_node ON e1_lease (node_id, epoch)");
        jdbc.execute("CREATE INDEX e1_lease_job ON e1_lease (job_key)");
        jdbc.execute("""
                CREATE TABLE e1_execution (
                    execution_id    uuid        NOT NULL,
                    job_key         text        NOT NULL,
                    tenant_id       text        NOT NULL DEFAULT '',
                    shard           smallint    NOT NULL,
                    priority        smallint    NOT NULL,
                    state           text        NOT NULL,
                    scheduled_at    timestamptz NOT NULL,
                    created_at      timestamptz NOT NULL,
                    finished_at     timestamptz,
                    actor           text        NOT NULL,
                    correlation_id  text,
                    idempotency_key text,
                    trace_id        text,
                    payload         jsonb       NOT NULL,
                    payload_type    text        NOT NULL,
                    PRIMARY KEY (created_at, execution_id)
                ) PARTITION BY RANGE (created_at)
                """);
        jdbc.execute("CREATE TABLE e1_execution_p0 PARTITION OF e1_execution FOR VALUES FROM ('%s') TO ('%s')"
                .formatted(NOW.minus(Duration.ofDays(1)), NOW.plus(Duration.ofDays(1))));
        jdbc.execute("CREATE INDEX e1_exec_id ON e1_execution (execution_id)");
        jdbc.execute("CREATE INDEX e1_exec_job ON e1_execution (job_key, created_at DESC)");
        jdbc.execute("CREATE INDEX e1_exec_corr ON e1_execution (correlation_id) WHERE correlation_id IS NOT NULL");
        jdbc.execute("""
                CREATE TABLE e1_attempt (
                    execution_id uuid        NOT NULL,
                    number       smallint    NOT NULL,
                    node_id      text        NOT NULL,
                    started_at   timestamptz NOT NULL,
                    finished_at  timestamptz NOT NULL,
                    outcome      text        NOT NULL,
                    error_type   text,
                    error        text,
                    PRIMARY KEY (finished_at, execution_id, number)
                ) PARTITION BY RANGE (finished_at)
                """);
        jdbc.execute("CREATE TABLE e1_attempt_p0 PARTITION OF e1_attempt FOR VALUES FROM ('%s') TO ('%s')"
                .formatted(NOW.minus(Duration.ofDays(1)), NOW.plus(Duration.ofDays(1))));
        jdbc.execute("CREATE INDEX e1_attempt_tp ON e1_attempt (finished_at, outcome)");
    }

    // ── infra comum ─────────────────────────────────────────────────────────

    /** Ownership §8.3 ({@code s % n == i}) — shards disjuntos por claimer, como o E3 validou. */
    private static List<Integer> ownedShards(int worker) {
        List<Integer> owned = new ArrayList<>();
        for (int s = 0; s < SHARD_COUNT; s++) {
            if (s % CLAIMERS == worker) {
                owned.add(s);
            }
        }
        return owned;
    }

    private interface WorkerBody {
        void run(int worker) throws Exception;
    }

    /** Propaga a primeira falha de conclusão com causa — sem isto uma exceção no dispatcher viraria só um assert de contagem no fim. */
    private static void awaitAll(Queue<Future<?>> futures) throws Exception {
        for (Future<?> future : futures) {
            future.get(20, TimeUnit.MINUTES);
        }
    }

    /**
     * Quem detecta a primeira falha cancela os pares e preserva a causa
     * (JCIP cap. 7) — sem o {@code shutdownNow}, os workers vivos drenariam
     * por minutos e um assert em {@code finally} enterraria o stack trace da
     * falha real (review do E1).
     */
    private void runWorkers(int workers, WorkerBody body) throws Exception {
        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        try {
            List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < workers; i++) {
                int worker = i;
                futures.add(executor.submit(() -> {
                    body.run(worker);
                    return null;
                }));
            }
            for (Future<?> future : futures) {
                future.get(20, TimeUnit.MINUTES);
            }
            executor.shutdown();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        } catch (Exception e) {
            executor.shutdownNow();
            throw e;
        }
    }

    /**
     * A rodada 2 revelou por que o loop de estabilização sozinho não basta:
     * backend idle do pool NÃO descarrega stats pendentes — o
     * {@code pgstat_report_stat} roda antes do sono do backend e o throttle
     * de 1s pula o flush de quem terminou o último comando há menos de 1s,
     * deixando ~16% dos contadores do braço group (drain de 3,7s) presos
     * para sempre; o loop então "converge" num estado estável-porém-
     * incompleto. Evictar as conexões idle encerra os backends, e backend
     * encerrado descarrega tudo ({@code pgstat_beshutdown_hook}) — o loop
     * de estabilização vira a espera pelo flush do shutdown.
     */
    private StatSnapshot settleAndSnapshot(HikariDataSource pool, JdbcTemplate jdbc) throws InterruptedException {
        pool.getHikariPoolMXBean().softEvictConnections();
        return StatSnapshot.take(jdbc);
    }

    private HikariDataSource pooledDataSource(DataSource raw) {
        HikariConfig config = new HikariConfig();
        config.setDataSource(raw);
        config.setMaximumPoolSize(POOL_SIZE);
        config.setConnectionTimeout(Duration.ofSeconds(10).toMillis());
        config.setPoolName("table-split-harness");
        return new HikariDataSource(config);
    }

    /** Relatório no stdout E em arquivo — o BASELINE referencia a tabela completa, e stdout de harness é efêmero. */
    private void printReport(List<RunResult> all, List<RunResult> medians) throws Exception {
        StringBuilder report = new StringBuilder();
        report.append("=== TableSplitExperimentHarness — E1 (executions=").append(EXECUTIONS)
                .append(", claimers=").append(CLAIMERS)
                .append(", batch=").append(CLAIM_BATCH)
                .append(", shards=").append(SHARD_COUNT)
                .append(", dispatch<=").append(DISPATCH_PERMITS)
                .append(", flush=").append(GROUP_FLUSH)
                .append(", payload=").append(PAYLOAD_JSON.length()).append("B")
                .append(", mediana de ").append(REPEATS).append(") ===\n\n");
        report.append("Todas as rodadas:\n");
        appendRuns(report, all);
        report.append("\nMedianas (por vazão de drain):\n");
        appendRuns(report, medians);

        RunResult current = medianOf(medians, Arm.CURRENT);
        RunResult sync = medianOf(medians, Arm.SPLIT_SYNC);
        RunResult group = medianOf(medians, Arm.SPLIT_GROUP);
        report.append("\nDeltas de tabela (mediana, ciclo completo, por execução):\n");
        appendTableDeltas(report, current, "mohs_");
        appendTableDeltas(report, sync, "e1_");
        appendTableDeltas(report, group, "e1_");

        double throughputGroup = group.drainRate() / current.drainRate();
        double throughputSync = sync.drainRate() / current.drainRate();
        double walDropGroup = 1 - group.lifecycleWalPerExec() / current.lifecycleWalPerExec();
        double walDropSync = 1 - sync.lifecycleWalPerExec() / current.lifecycleWalPerExec();
        double e2eGroup = (EXECUTIONS / (group.enqueue().seconds() + group.drain().seconds()))
                / (EXECUTIONS / (current.enqueue().seconds() + current.drain().seconds()));
        report.append("\nVereditos E1 (kill: vazão < 1.5x OU queda de WAL < 40%):\n");
        report.append("  vazão drain  split-group/current = %.2fx   (split-sync: %.2fx)%n"
                .formatted(throughputGroup, throughputSync));
        report.append("  vazão fim a fim (enqueue+drain) split-group/current = %.2fx — o §20.3 lê end-to-end;%n"
                .formatted(e2eGroup));
        report.append("    o enqueue do split é 2 INSERTs/commit por construção (§7.5), custo do chamador\n");
        report.append("  WAL/execução ciclo completo: current %.0f B → split-group %.0f B (−%.1f%%; split-sync %.0f B, −%.1f%%)%n"
                .formatted(current.lifecycleWalPerExec(), group.lifecycleWalPerExec(), walDropGroup * 100,
                        sync.lifecycleWalPerExec(), walDropSync * 100));
        report.append("  tuple versions na tabela grande/execução (§3.3, falsifica se > 3): current %.2f → split-group %.2f%n"
                .formatted(current.bigTableVersionsPerExec(), group.bigTableVersionsPerExec()));

        System.out.println();
        System.out.print(report);
        System.out.println();
        Path out = Path.of("docs", "performance", "e1-table-split-results.txt");
        Files.createDirectories(out.getParent());
        Files.writeString(out, report.toString());
    }

    private static void appendRuns(StringBuilder report, List<RunResult> runs) {
        report.append("%-12s %-8s %9s %11s %12s %13s %8s%n".formatted(
                "arm", "phase", "s", "exec/s", "WAL B/exec", "WAL rec/exec", "FPI"));
        for (RunResult run : runs) {
            appendPhase(report, run.arm().label, "enqueue", run.executions(), run.enqueue());
            appendPhase(report, run.arm().label, "drain", run.executions(), run.drain());
        }
    }

    private static void appendPhase(StringBuilder report, String arm, String phase, int executions,
            PhaseResult result) {
        report.append("%-12s %-8s %9.1f %11.1f %12.1f %13.2f %8d%n".formatted(
                arm, phase, result.seconds(), executions / result.seconds(),
                result.walBytes() / (double) executions, result.walRecords() / (double) executions,
                result.walFpi()));
    }

    private static void appendTableDeltas(StringBuilder report, RunResult run, String prefix) {
        report.append("  %s:%n".formatted(run.arm().label));
        Map<String, TableStat> lifecycle = new LinkedHashMap<>(run.enqueue().tableDeltas());
        run.drain().tableDeltas().forEach((name, stat) -> lifecycle.merge(name, stat, TableStat::plus));
        double executions = run.executions();
        lifecycle.entrySet().stream()
                .filter(e -> e.getKey().startsWith(prefix))
                .filter(e -> e.getValue().hasWrites())
                .forEach(e -> {
                    TableStat stat = e.getValue();
                    report.append("    %-24s ins %.3f  upd %.3f (hot %.3f)  del %.3f  dead_tup %d%n".formatted(
                            e.getKey(), stat.ins() / executions, stat.upd() / executions,
                            stat.hotUpd() / executions, stat.del() / executions, stat.deadTup()));
                });
    }

    private static RunResult medianOf(List<RunResult> medians, Arm arm) {
        return medians.stream().filter(r -> r.arm() == arm).findFirst().orElseThrow();
    }

}
