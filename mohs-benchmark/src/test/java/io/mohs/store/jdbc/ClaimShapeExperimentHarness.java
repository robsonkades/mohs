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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import tools.jackson.databind.json.JsonMapper;

import io.github.robsonkades.uuidv7.UUIDv7;
import io.mohs.core.definition.JobDefinition;
import io.mohs.core.execution.Execution;
import io.mohs.core.execution.Priority;
import io.mohs.engine.ExecutionWindowRegistry;
import io.mohs.store.jdbc.ClaimQueryLoadHarness.LatencyStats;
import io.mohs.store.jdbc.dialect.PostgresJdbcDialect;
import io.mohs.test.MutableClock;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Experimentos E2/E3 do ARCHITECTURE_REDESIGN_PLAN (§20.3), Postgres
 * (Tier 1) — decidem a ADR-A (§5.4) e a ADR-F (§8.3) ANTES de qualquer
 * código de produção do redesign:
 *
 * <p><b>E2</b> — a forma nova do claim ({@code DELETE … RETURNING} +
 * {@code INSERT} numa CTE sobre {@code e2_ready}/{@code e2_lease},
 * <i>incluindo</i> a leitura batched de payload na tabela particionada —
 * sem ela a comparação mediria uma query que não existe) contra a forma
 * atual ({@link JdbcClaimer} real sobre {@code schema-postgresql.sql}),
 * a 1/4/8/16 claimers concorrentes. Kill criterion: nova forma
 * &lt; 1,3× a 8 claimers.
 *
 * <p><b>E3</b> — a mesma forma nova com {@code SHARD_COUNT} 1 vs 64:
 * onde começa o convoy do {@code SKIP LOCKED} e se o sharding o remove.
 * Kill criterion: se o não-shardado já escala linear até 16, sharding é
 * prematuro.
 *
 * <p>Mesmo container para todos os braços (comparabilidade de hardware);
 * estado zerado por braço; WAL bytes por linha reivindicada via delta de
 * {@code pg_stat_wal} (o {@code UPDATE} de hoje vs {@code DELETE+INSERT}
 * da forma nova têm físicas de escrita diferentes — o número decide, não
 * a intuição). Roda só por {@code -Dtest=} explícito, como todo harness.
 */
class ClaimShapeExperimentHarness {

    private static final Instant NOW = Instant.parse("2026-08-21T12:00:00Z");
    private static final Duration LEASE_TTL = Duration.ofSeconds(30);

    private static final int BATCH_SIZE = 100;
    private static final int BACKLOG = 100_000;
    private static final int JOB_COUNT = 100;
    private static final int[] CLAIMER_SWEEP = { 1, 4, 8, 16 };
    private static final int[] SHARD_SWEEP = { 1, 64 };

    /**
     * A forma §5.4 literal — inclusive o predicado {@code :inadmissible}
     * (vazio aqui, mas presente: medir sem ele mediria outra query). Duas
     * variantes de predicado de shard, porque a primeira rodada do
     * experimento MATOU a forma do plano: com {@code shard = ANY(...)}
     * multi-shard, o índice {@code (shard, priority, visible_at)} não
     * fornece a ordem {@code priority, visible_at} — o planner faz bitmap
     * scan de TODO o backlog elegível + sort externo por rodada (EXPLAIN na
     * primeira captura). {@code SINGLE} é a correção candidata: um shard por
     * rodada ({@code shard = ?}, igualdade única, scan ordenado pelo
     * índice), round-robin sobre os shards do claimer — prioridade continua
     * global DENTRO do shard, que é o que §8.3 já aceita.
     */
    private static final String TARGET_CLAIM_ANY = """
            WITH picked AS (
                SELECT execution_id, job_key, attempt
                  FROM e2_ready
                 WHERE shard = ANY(%s)
                   AND visible_at <= ?
                   AND job_key <> ALL('{}'::text[])
                 ORDER BY priority, visible_at
                 LIMIT ?
                   FOR UPDATE SKIP LOCKED
            ),
            gone AS (
                DELETE FROM e2_ready r USING picked p
                 WHERE r.execution_id = p.execution_id
             RETURNING r.execution_id, r.job_key, r.attempt
            )
            INSERT INTO e2_lease (execution_id, job_key, node_id, epoch, attempt_number, claimed_at)
            SELECT execution_id, job_key, ?, 1, attempt, ? FROM gone
            RETURNING execution_id
            """;

    private static final String TARGET_CLAIM_SINGLE = """
            WITH picked AS (
                SELECT execution_id, job_key, attempt
                  FROM e2_ready
                 WHERE shard = ?
                   AND visible_at <= ?
                   AND job_key <> ALL('{}'::text[])
                 ORDER BY priority, visible_at
                 LIMIT ?
                   FOR UPDATE SKIP LOCKED
            ),
            gone AS (
                DELETE FROM e2_ready r USING picked p
                 WHERE r.execution_id = p.execution_id
             RETURNING r.execution_id, r.job_key, r.attempt
            )
            INSERT INTO e2_lease (execution_id, job_key, node_id, epoch, attempt_number, claimed_at)
            SELECT execution_id, job_key, ?, 1, attempt, ? FROM gone
            RETURNING execution_id
            """;

    private enum ShardScan {
        ANY_OWNED("any"), ROUND_ROBIN_SINGLE("rr");

        final String arm;

        ShardScan(String arm) {
            this.arm = arm;
        }
    }

    private static final String TARGET_PAYLOAD_SQL = """
            SELECT execution_id, payload, payload_type FROM e2_execution
             WHERE execution_id = ANY(?) AND created_at >= ? AND created_at < ?
            """;

    record Handler() {
    }

    record SweepResult(String arm, int shards, int claimers, double rowsPerSecond,
            double walBytesPerRow, LatencyStats roundLatency) {
    }

    /**
     * Repetições nas células que carregam veredito (claimers ≥ 8): a 527k
     * rows/s o sweep dura ~0,2s — janela menor que o período do ruído
     * dominante (checkpoint do container) não separa convoy de azar com uma
     * amostra só (review da Fase 1). Mediana de 3, não média: um checkpoint
     * no meio de UMA rodada não contamina o valor reportado.
     */
    private static final int VERDICT_REPEATS = 3;

    @Test
    void run() throws Exception {
        // aquecimento descartado: a rodada 1 do experimento mediu current@1
        // com p99 de 238ms — puro C2/buffers frios da primeira varredura da
        // JVM, não custo da query (§12.5 do plano manda não pagar isso)
        measureCurrentShape(4);
        measureTargetShape(ShardScan.ROUND_ROBIN_SINGLE, 64, 4);

        List<SweepResult> results = new ArrayList<>();
        for (int claimers : CLAIMER_SWEEP) {
            results.add(medianSweep(claimers, () -> measureCurrentShape(claimers)));
        }
        for (ShardScan mode : ShardScan.values()) {
            for (int shards : SHARD_SWEEP) {
                for (int claimers : CLAIMER_SWEEP) {
                    results.add(medianSweep(claimers, () -> measureTargetShape(mode, shards, claimers)));
                }
            }
        }
        captureExplain();
        printReport(results);
    }

    private SweepResult medianSweep(int claimers, Callable<SweepResult> sweep) throws Exception {
        int repeats = claimers >= 8 ? VERDICT_REPEATS : 1;
        List<SweepResult> runs = new ArrayList<>(repeats);
        for (int i = 0; i < repeats; i++) {
            runs.add(sweep.call());
        }
        runs.sort(Comparator.comparingDouble(SweepResult::rowsPerSecond));
        return runs.get(runs.size() / 2);
    }

    // ── braço A: a forma de hoje, pelo caminho real ─────────────────────────

    private SweepResult measureCurrentShape(int claimers) throws Exception {
        DataSource raw = PostgresTestSupport.freshSchema();
        try (HikariDataSource pool = pooledDataSource(raw, claimers)) {
            MutableClock clock = new MutableClock(NOW, ZoneId.of("UTC"));
            JdbcTemplate jdbc = new JdbcTemplate(pool);
            JdbcJobStore jobStore = new JdbcJobStore(pool, clock);
            JdbcExecutionStore executionStore = new JdbcExecutionStore(pool, clock, JsonMapper.builder().build(),
                    new PostgresJdbcDialect());
            seedCurrentBacklog(jdbc, jobStore);

            List<Long> roundNanos = Collections.synchronizedList(new ArrayList<>());
            AtomicInteger claimedTotal = new AtomicInteger();
            PgStatSnapshot before = PgStatSnapshot.take(jdbc);
            long start = System.nanoTime();
            runClaimers(claimers, nodeIndex -> {
                JdbcClaimer claimer = new JdbcClaimer(pool, new PostgresJdbcDialect(), clock, executionStore,
                        jobStore, LEASE_TTL, new ExecutionWindowRegistry(List.of()),
                        new JdbcRateLimitStore(pool, clock));
                String nodeId = "current-node-" + nodeIndex;
                List<Execution> claimed;
                do {
                    long roundStart = System.nanoTime();
                    claimed = claimer.claim(nodeId, BATCH_SIZE);
                    roundNanos.add(System.nanoTime() - roundStart);
                    claimedTotal.addAndGet(claimed.size());
                } while (!claimed.isEmpty());
            });
            double seconds = (System.nanoTime() - start) / 1_000_000_000.0;
            PgStatSnapshot after = PgStatSnapshot.take(jdbc);
            assertThat(claimedTotal.get()).isEqualTo(BACKLOG);
            return new SweepResult("current", 0, claimers, BACKLOG / seconds,
                    (after.walBytes() - before.walBytes()) / (double) BACKLOG, latencyOf(roundNanos));
        }
    }

    /** Mesmo seed misto do {@link ClaimQueryLoadHarness} (~20% RETRY_WAITING — desde a ADR-0033 retry é claimável). */
    private void seedCurrentBacklog(JdbcTemplate jdbc, JdbcJobStore jobStore) {
        List<Object[]> rows = new ArrayList<>(BACKLOG);
        int perJob = BACKLOG / JOB_COUNT;
        for (int j = 0; j < JOB_COUNT; j++) {
            String jobKey = "e2-job-" + j;
            jobStore.upsert(JobDefinition.of(jobKey, Handler.class, spec -> spec.onDemand()));
            for (int e = 0; e < perJob; e++) {
                String state = e % 5 == 0 ? "RETRY_WAITING" : "ENQUEUED";
                rows.add(new Object[] {
                        UUIDv7.randomUUID().toString(), jobKey, state,
                        JdbcTimestamps.toUtcLocalDateTime(NOW.minusSeconds(1)),
                        Priority.NORMAL.value(), JdbcTimestamps.toUtcLocalDateTime(NOW)
                });
            }
        }
        jdbc.batchUpdate("""
                INSERT INTO mohs_executions (
                    id, job_key, state, scheduled_at, actor, priority, payload, payload_type, created_at)
                VALUES (?, ?, ?, ?, 'harness', ?, '{}', 'java.lang.Object', ?)
                """, rows);
        // estatísticas frescas nos dois braços — sem isto o plano da primeira
        // rodada é sorteado pelo autoanalyze no meio do drain (lição da rodada
        // 08-15 do BASELINE)
        jdbc.execute("ANALYZE mohs_executions, mohs_job_definitions");
    }

    // ── braço B: a forma-alvo (§5.4), com leitura de payload ────────────────

    private SweepResult measureTargetShape(ShardScan mode, int shardCount, int claimers) throws Exception {
        DataSource raw = PostgresTestSupport.freshSchema();
        try (HikariDataSource pool = pooledDataSource(raw, claimers)) {
            JdbcTemplate jdbc = new JdbcTemplate(pool);
            createTargetSchema(jdbc);
            seedTargetBacklog(jdbc, shardCount);

            List<Long> roundNanos = Collections.synchronizedList(new ArrayList<>());
            AtomicInteger claimedTotal = new AtomicInteger();
            PgStatSnapshot before = PgStatSnapshot.take(jdbc);
            long start = System.nanoTime();
            runClaimers(claimers, nodeIndex -> {
                List<Integer> owned = ownedShards(shardCount, claimers, nodeIndex);
                String nodeId = "target-node-" + nodeIndex;
                if (mode == ShardScan.ANY_OWNED) {
                    drainAnyOwned(pool, owned, nodeId, roundNanos, claimedTotal);
                } else {
                    drainRoundRobin(pool, owned, nodeId, roundNanos, claimedTotal);
                }
            });
            double seconds = (System.nanoTime() - start) / 1_000_000_000.0;
            PgStatSnapshot after = PgStatSnapshot.take(jdbc);
            // com ownership particionado (shard 64) um claimer só esvazia os
            // próprios shards — o total confere no agregado, não por claimer
            assertThat(claimedTotal.get()).isEqualTo(BACKLOG);
            return new SweepResult(mode.arm, shardCount, claimers, BACKLOG / seconds,
                    (after.walBytes() - before.walBytes()) / (double) BACKLOG, latencyOf(roundNanos));
        }
    }

    private void drainAnyOwned(DataSource pool, List<Integer> owned, String nodeId,
            List<Long> roundNanos, AtomicInteger claimedTotal) {
        String ownedLiteral = arrayLiteral(owned);
        int claimed;
        do {
            long roundStart = System.nanoTime();
            claimed = claimRound(pool, TARGET_CLAIM_ANY.formatted(ownedLiteral), ps -> {
                ps.setTimestamp(1, Timestamp.from(NOW));
                ps.setInt(2, BATCH_SIZE);
                ps.setString(3, nodeId);
                ps.setTimestamp(4, Timestamp.from(NOW));
            });
            roundNanos.add(System.nanoTime() - roundStart);
            claimedTotal.addAndGet(claimed);
        } while (claimed > 0);
    }

    /**
     * Um shard por rodada, girando sobre os shards do claimer; termina após
     * uma volta inteira vazia. Sondas vazias entram na latência de rodada de
     * propósito — são parte do custo real do modo.
     */
    private void drainRoundRobin(DataSource pool, List<Integer> owned, String nodeId,
            List<Long> roundNanos, AtomicInteger claimedTotal) {
        int cursor = 0;
        int consecutiveEmpty = 0;
        while (consecutiveEmpty < owned.size()) {
            int shard = owned.get(cursor);
            cursor = (cursor + 1) % owned.size();
            long roundStart = System.nanoTime();
            int claimed = claimRound(pool, TARGET_CLAIM_SINGLE, ps -> {
                ps.setInt(1, shard);
                ps.setTimestamp(2, Timestamp.from(NOW));
                ps.setInt(3, BATCH_SIZE);
                ps.setString(4, nodeId);
                ps.setTimestamp(5, Timestamp.from(NOW));
            });
            roundNanos.add(System.nanoTime() - roundStart);
            claimedTotal.addAndGet(claimed);
            consecutiveEmpty = claimed == 0 ? consecutiveEmpty + 1 : 0;
        }
    }

    /**
     * Ownership §8.3: {@code s % n == i}. Com {@code SHARD_COUNT=1} todos os
     * claimers disputam o shard único — é o modo "sem sharding" do E3, o
     * {@code SKIP LOCKED} resolve, exatamente como hoje.
     */
    private static List<Integer> ownedShards(int shardCount, int claimers, int nodeIndex) {
        List<Integer> owned = new ArrayList<>();
        for (int s = 0; s < shardCount; s++) {
            if (shardCount == 1 || s % claimers == nodeIndex) {
                owned.add(s);
            }
        }
        return owned;
    }

    private static String arrayLiteral(List<Integer> shards) {
        return shards.stream()
                .map(String::valueOf)
                .collect(Collectors.joining(",", "'{", "}'::smallint[]"));
    }

    private interface Binder {
        void bind(PreparedStatement ps) throws SQLException;
    }

    /** Uma rodada completa da forma-alvo: claim CTE + leitura batched dos payloads (o custo escondido que o E2 exige medir). */
    private int claimRound(DataSource pool, String claimSql, Binder binder) {
        try (Connection connection = pool.getConnection()) {
            List<UUID> claimed = new ArrayList<>(BATCH_SIZE);
            try (PreparedStatement ps = connection.prepareStatement(claimSql)) {
                binder.bind(ps);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        claimed.add(rs.getObject(1, UUID.class));
                    }
                }
            }
            if (claimed.isEmpty()) {
                return 0;
            }
            // em produção os bounds saem do timestamp embutido no UUIDv7 (§5.4);
            // aqui o seed é de janela única e os bounds são a janela do seed
            try (PreparedStatement ps = connection.prepareStatement(TARGET_PAYLOAD_SQL)) {
                Array ids = connection.createArrayOf("uuid", claimed.toArray());
                ps.setArray(1, ids);
                ps.setTimestamp(2, Timestamp.from(NOW.minus(Duration.ofHours(1))));
                ps.setTimestamp(3, Timestamp.from(NOW.plus(Duration.ofHours(1))));
                int payloads = 0;
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        payloads++;
                    }
                }
                assertThat(payloads).isEqualTo(claimed.size());
            }
            return claimed.size();
        } catch (SQLException e) {
            throw new IllegalStateException("target claim round failed", e);
        }
    }

    private void createTargetSchema(JdbcTemplate jdbc) {
        jdbc.execute("DROP TABLE IF EXISTS e2_ready, e2_lease, e2_execution CASCADE");
        jdbc.execute("""
                CREATE TABLE e2_ready (
                    execution_id uuid        NOT NULL PRIMARY KEY,
                    job_key      text        NOT NULL,
                    shard        smallint    NOT NULL,
                    priority     smallint    NOT NULL,
                    attempt      smallint    NOT NULL,
                    visible_at   timestamptz NOT NULL
                ) WITH (fillfactor = 70)
                """);
        jdbc.execute("CREATE INDEX e2_ready_claim ON e2_ready (shard, priority, visible_at)"
                + " INCLUDE (execution_id, job_key, attempt)");
        jdbc.execute("""
                CREATE TABLE e2_lease (
                    execution_id   uuid        NOT NULL PRIMARY KEY,
                    job_key        text        NOT NULL,
                    node_id        text        NOT NULL,
                    epoch          bigint      NOT NULL,
                    attempt_number smallint    NOT NULL,
                    claimed_at     timestamptz NOT NULL
                ) WITH (fillfactor = 70)
                """);
        jdbc.execute("""
                CREATE TABLE e2_execution (
                    execution_id uuid        NOT NULL,
                    created_at   timestamptz NOT NULL,
                    payload      text        NOT NULL,
                    payload_type text        NOT NULL,
                    PRIMARY KEY (created_at, execution_id)
                ) PARTITION BY RANGE (created_at)
                """);
        jdbc.execute("CREATE TABLE e2_execution_p0 PARTITION OF e2_execution FOR VALUES FROM ('%s') TO ('%s')"
                .formatted(NOW.minus(Duration.ofDays(1)), NOW.plus(Duration.ofDays(1))));
        jdbc.execute("CREATE INDEX e2_exec_id ON e2_execution (execution_id)");
    }

    private void seedTargetBacklog(JdbcTemplate jdbc, int shardCount) {
        List<Object[]> readyRows = new ArrayList<>(BACKLOG);
        List<Object[]> executionRows = new ArrayList<>(BACKLOG);
        Timestamp createdAt = Timestamp.from(NOW);
        Timestamp visibleAt = Timestamp.from(NOW.minusSeconds(1));
        for (int i = 0; i < BACKLOG; i++) {
            UUID id = UUIDv7.randomUUID();
            String jobKey = "e2-job-" + (i % JOB_COUNT);
            int shard = Math.floorMod(id.hashCode(), shardCount);
            readyRows.add(new Object[] { id, jobKey, shard, Priority.NORMAL.value(), 1, visibleAt });
            executionRows.add(new Object[] { id, createdAt });
        }
        jdbc.batchUpdate("INSERT INTO e2_execution (execution_id, created_at, payload, payload_type)"
                + " VALUES (?, ?, '{}', 'java.lang.Object')", executionRows);
        jdbc.batchUpdate("INSERT INTO e2_ready (execution_id, job_key, shard, priority, attempt, visible_at)"
                + " VALUES (?, ?, ?, ?, ?, ?)", readyRows);
        jdbc.execute("ANALYZE e2_ready, e2_execution");
    }

    // ── infra comum ─────────────────────────────────────────────────────────

    private interface ClaimerBody {
        void run(int nodeIndex) throws Exception;
    }

    private void runClaimers(int claimers, ClaimerBody body) throws Exception {
        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        try {
            List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < claimers; i++) {
                int nodeIndex = i;
                futures.add(executor.submit(() -> {
                    body.run(nodeIndex);
                    return null;
                }));
            }
            for (Future<?> future : futures) {
                future.get(300, TimeUnit.SECONDS);
            }
        } finally {
            executor.shutdown();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    private HikariDataSource pooledDataSource(DataSource raw, int claimers) {
        HikariConfig config = new HikariConfig();
        config.setDataSource(raw);
        config.setMaximumPoolSize(claimers * 2);
        config.setConnectionTimeout(Duration.ofSeconds(3).toMillis());
        config.setPoolName("claim-shape-harness");
        return new HikariDataSource(config);
    }

    private static LatencyStats latencyOf(List<Long> roundNanos) {
        return LatencyStats.of(roundNanos.stream().mapToLong(Long::longValue).toArray());
    }

    /**
     * {@code pg_stat} lag: o coletor (PG 15+, shared memory) descarrega por
     * backend em até ~1s — o sono antes do snapshot final é isso, não
     * sincronização de teste.
     */
    record PgStatSnapshot(long walBytes) {

        static PgStatSnapshot take(JdbcTemplate jdbc) throws InterruptedException {
            Thread.sleep(1200);
            Long wal = jdbc.queryForObject("SELECT wal_bytes FROM pg_stat_wal", Long.class);
            return new PgStatSnapshot(wal == null ? 0 : wal);
        }
    }

    private void captureExplain() throws Exception {
        DataSource raw = PostgresTestSupport.freshSchema();
        try (HikariDataSource pool = pooledDataSource(raw, 1)) {
            JdbcTemplate jdbc = new JdbcTemplate(pool);
            createTargetSchema(jdbc);
            seedTargetBacklog(jdbc, 64);
            StringBuilder capture = new StringBuilder();
            explainOne(jdbc, capture, "claim ANY (ownership 8/64 shards) — a forma §5.4 v1", explainableAny());
            explainOne(jdbc, capture, "claim shard único (round-robin) — a correção candidata", explainableSingle());
            Path out = Path.of("docs", "performance", "explain-e2-ready-claim.txt");
            Files.createDirectories(out.getParent());
            Files.writeString(out, capture.toString());
        }
    }

    private void explainOne(JdbcTemplate jdbc, StringBuilder capture, String label, String sql) {
        List<String> plan = jdbc.query("EXPLAIN (ANALYZE, BUFFERS) " + sql, (rs, i) -> rs.getString(1));
        String header = "=== EXPLAIN (ANALYZE, BUFFERS) — " + label + " ===";
        System.out.println();
        System.out.println(header);
        plan.forEach(System.out::println);
        capture.append(header).append(System.lineSeparator())
                .append(String.join(System.lineSeparator(), plan))
                .append(System.lineSeparator()).append(System.lineSeparator());
    }

    /**
     * As CTEs com literais no lugar dos binds — EXPLAIN não aceita
     * parâmetros sem PREPARE, e o plano é o mesmo. O braço ANY é capturado
     * com o ownership de 8 claimers (8 dos 64 shards) — a configuração que
     * o veredito E2 cita, não a de nó único (review da Fase 1).
     */
    private String explainableAny() {
        return inlineCommonBinds(TARGET_CLAIM_ANY.formatted(arrayLiteral(ownedShards(64, 8, 0))), "explain-node");
    }

    private String explainableSingle() {
        return inlineCommonBinds(TARGET_CLAIM_SINGLE.replaceFirst("shard = \\?", "shard = 7"), "explain-node-2");
    }

    private static String inlineCommonBinds(String claimSql, String nodeId) {
        return claimSql
                .replaceFirst("visible_at <= \\?", "visible_at <= '" + NOW + "'")
                .replaceFirst("LIMIT \\?", "LIMIT " + BATCH_SIZE)
                .replaceFirst("\\?, 1, attempt, \\?", "'" + nodeId + "', 1, attempt, '" + NOW + "'");
    }

    /** Relatório no stdout E em arquivo — o BASELINE referencia a tabela completa, e stdout de harness é efêmero. */
    private void printReport(List<SweepResult> results) throws Exception {
        StringBuilder report = new StringBuilder();
        report.append("=== ClaimShapeExperimentHarness — E2/E3 (batch=").append(BATCH_SIZE)
                .append(", backlog=").append(BACKLOG)
                .append(", mediana de ").append(VERDICT_REPEATS).append(" p/ claimers >= 8) ===\n");
        report.append("%-8s %7s %9s %14s %12s %10s %10s%n".formatted(
                "arm", "shards", "claimers", "rows/s", "WAL B/row", "p50(ms)", "p99(ms)"));
        for (SweepResult r : results) {
            report.append("%-8s %7s %9d %14.1f %12.1f %10.2f %10.2f%n".formatted(
                    r.arm(), r.shards() == 0 ? "-" : String.valueOf(r.shards()), r.claimers(),
                    r.rowsPerSecond(), r.walBytesPerRow(), r.roundLatency().p50Ms(), r.roundLatency().p99Ms()));
        }
        double current8 = rowsPerSecond(results, "current", 0, 8);
        report.append('\n');
        for (ShardScan mode : ShardScan.values()) {
            for (int shards : SHARD_SWEEP) {
                report.append("E2: %s(shard=%d) / current @ 8 claimers = %.2fx  (kill criterion: < 1.3x)%n".formatted(
                        mode.arm, shards, rowsPerSecond(results, mode.arm, shards, 8) / current8));
            }
        }
        report.append("E3: rr(64) / rr(1) @ 8 claimers = %.2fx%n".formatted(
                rowsPerSecond(results, "rr", 64, 8) / rowsPerSecond(results, "rr", 1, 8)));
        System.out.println();
        System.out.print(report);
        System.out.println();
        Path out = Path.of("docs", "performance", "e2-e3-claim-shape-results.txt");
        Files.createDirectories(out.getParent());
        Files.writeString(out, report.toString());
    }

    private static double rowsPerSecond(List<SweepResult> results, String arm, int shards, int claimers) {
        return results.stream()
                .filter(r -> r.arm().equals(arm) && r.shards() == shards && r.claimers() == claimers)
                .findFirst().orElseThrow().rowsPerSecond();
    }

}
