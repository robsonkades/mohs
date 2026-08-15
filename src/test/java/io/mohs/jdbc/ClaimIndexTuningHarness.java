package io.mohs.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.sql.DataSource;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import tools.jackson.databind.json.JsonMapper;

import io.mohs.core.definition.JobDefinition;
import io.mohs.core.execution.Execution;
import io.mohs.core.execution.Priority;
import io.mohs.engine.ExecutionWindowRegistry;
import io.mohs.jdbc.dialect.JdbcDialect;
import io.mohs.jdbc.dialect.PostgresJdbcDialect;
import io.mohs.jdbc.dialect.SqlServerJdbcDialect;
import io.mohs.test.MutableClock;

/**
 * Gate de benchmark da DBTUNE-5 do {@code docs/codereview-tuning.md} —
 * índice parcial/filtrado (Postgres/SQL Server) pra {@code
 * idx_mohs_executions_claim}, servindo só o backlog vivo ({@code state =
 * 'ENQUEUED'}) em vez da tabela inteira. MySQL/H2 não têm índice
 * parcial — sem mudança nesses dois dialetos.
 *
 * <p>Mede duas coisas, antes/depois, no mesmo container (histórico
 * mantido constante — só a definição do índice muda no meio): tamanho do
 * índice (prova direta do "índice contém só o backlog vivo") e
 * throughput do cenário de 8 nós concorrentes sobre um backlog fresco de
 * 3000 linhas, com {@link #HISTORY_ROWS} execuções terminais já
 * acumuladas na tabela — cenário que o {@code ClaimQueryLoadHarness}
 * original não exercita (backlog limpo, sem história).
 *
 * <p>Dois métodos de teste separados (não um {@code run()} único) pra
 * uma falha/timeout num dialeto não jogar fora o resultado do outro, e
 * pra dar pra rodar um de cada vez: {@code mvn test
 * -Dtest=ClaimIndexTuningHarness#postgres}. Log de progresso em cada
 * fase — nada de fase silenciosa de minutos sem eco nenhum.
 */
class ClaimIndexTuningHarness {

    record Handler() {
    }

    private static final Instant NOW = Instant.parse("2026-08-14T12:00:00Z");
    private static final Duration LEASE_TTL = Duration.ofSeconds(30);
    private static final int BATCH_SIZE = 20;
    private static final int CONCURRENT_NODES = 8;
    private static final int SEED_POOL_SIZE = 16;

    private static final int HISTORY_ROWS = 20_000;
    private static final int HISTORY_JOB_COUNT = 500;
    private static final int HISTORY_BATCH_CHUNK = 2_000;

    private static final int BACKLOG_JOB_COUNT = 300;
    private static final int BACKLOG_EXECUTIONS_PER_JOB = 10; // 3000 linhas — mesma escala do ClaimQueryLoadHarness

    @Test
    void postgres() throws Exception {
        printReport(measure("PostgreSQL", PostgresTestSupport.freshSchema(), new PostgresJdbcDialect(),
                "DROP INDEX idx_mohs_executions_claim",
                "CREATE INDEX idx_mohs_executions_claim ON mohs_executions (priority, scheduled_at) WHERE state = 'ENQUEUED'",
                "SELECT pg_relation_size('idx_mohs_executions_claim')"));
    }

    @Test
    void sqlServer() throws Exception {
        printReport(measure("SQL Server", SqlServerTestSupport.freshSchema(), new SqlServerJdbcDialect(),
                "DROP INDEX idx_mohs_executions_claim ON mohs_executions",
                "CREATE INDEX idx_mohs_executions_claim ON mohs_executions (priority, scheduled_at) WHERE state = 'ENQUEUED'",
                """
                SELECT SUM(ps.used_page_count) * 8 * 1024
                FROM sys.dm_db_partition_stats ps
                JOIN sys.indexes i ON ps.object_id = i.object_id AND ps.index_id = i.index_id
                WHERE i.object_id = OBJECT_ID('mohs_executions') AND i.name = 'idx_mohs_executions_claim'
                """));
    }

    private DialectResult measure(String label, DataSource raw, JdbcDialect dialect,
            String dropIndexSql, String createFilteredIndexSql, String indexSizeSql) throws Exception {
        MutableClock clock = new MutableClock(NOW, ZoneId.of("UTC"));
        try (HikariDataSource pool = seedPool(raw)) {
            log(label, "seeding " + HISTORY_ROWS + " history rows...");
            seedHistory(pool, clock);
            log(label, "history seeded, seeding backlog (before)...");
            seedBacklog(pool, clock, "before");

            long sizeBeforeBytes = indexSizeBytes(raw, indexSizeSql);
            log(label, "index size before = " + sizeBeforeBytes + " bytes, measuring throughput...");
            double throughputBefore = measureThroughput(pool, dialect, clock);
            log(label, "throughput before = " + throughputBefore + " rows/s, swapping index...");

            try (Connection connection = raw.getConnection(); Statement statement = connection.createStatement()) {
                statement.execute(dropIndexSql);
                statement.execute(createFilteredIndexSql);
            }
            log(label, "index swapped, seeding backlog (after)...");
            seedBacklog(pool, clock, "after");

            long sizeAfterBytes = indexSizeBytes(raw, indexSizeSql);
            log(label, "index size after = " + sizeAfterBytes + " bytes, measuring throughput...");
            double throughputAfter = measureThroughput(pool, dialect, clock);
            log(label, "throughput after = " + throughputAfter + " rows/s, done.");

            return new DialectResult(label, sizeBeforeBytes, sizeAfterBytes, throughputBefore, throughputAfter);
        }
    }

    private static void log(String label, String message) {
        System.out.println("[" + label + "] " + message);
    }

    private HikariDataSource seedPool(DataSource raw) {
        HikariConfig config = new HikariConfig();
        config.setDataSource(raw);
        config.setMaximumPoolSize(SEED_POOL_SIZE);
        config.setConnectionTimeout(Duration.ofSeconds(5).toMillis());
        config.setPoolName("claim-index-tuning-seed");
        return new HikariDataSource(config);
    }

    /** Execuções terminais (nunca {@code ENQUEUED}) — peso morto que só a versão cheia do índice carrega. */
    private void seedHistory(DataSource pool, MutableClock clock) {
        JdbcJobStore jobStore = new JdbcJobStore(pool, clock);
        JdbcTemplate rawJdbcTemplate = new JdbcTemplate(pool);
        List<String> jobKeys = new ArrayList<>(HISTORY_JOB_COUNT);
        for (int j = 0; j < HISTORY_JOB_COUNT; j++) {
            String jobKey = "history-job-" + j;
            jobStore.upsert(JobDefinition.of(jobKey, Handler.class, spec -> spec.onDemand()));
            jobKeys.add(jobKey);
        }

        List<Object[]> chunk = new ArrayList<>(HISTORY_BATCH_CHUNK);
        for (int i = 0; i < HISTORY_ROWS; i++) {
            String jobKey = jobKeys.get(i % jobKeys.size());
            Timestamp at = JdbcTimestamps.toUtcTimestamp(NOW.minusSeconds(HISTORY_ROWS - i));
            chunk.add(new Object[] {
                    UUID.randomUUID().toString(), jobKey, at, at, at
            });
            if (chunk.size() == HISTORY_BATCH_CHUNK) {
                flushHistoryChunk(rawJdbcTemplate, chunk);
                chunk.clear();
            }
        }
        if (!chunk.isEmpty()) {
            flushHistoryChunk(rawJdbcTemplate, chunk);
        }
    }

    private void flushHistoryChunk(JdbcTemplate rawJdbcTemplate, List<Object[]> chunk) {
        rawJdbcTemplate.batchUpdate("""
                INSERT INTO mohs_executions (
                    id, job_key, state, scheduled_at, fired_at, actor, payload, payload_type, created_at)
                VALUES (?, ?, 'SUCCEEDED', ?, ?, 'harness', '{}', 'java.lang.Object', ?)
                """, chunk);
    }

    private void seedBacklog(DataSource pool, MutableClock clock, String prefix) {
        JdbcJobStore jobStore = new JdbcJobStore(pool, clock);
        JdbcTemplate rawJdbcTemplate = new JdbcTemplate(pool);
        List<Object[]> batchArgs = new ArrayList<>();
        for (int j = 0; j < BACKLOG_JOB_COUNT; j++) {
            String jobKey = prefix + "-backlog-job-" + j;
            jobStore.upsert(JobDefinition.of(jobKey, Handler.class, spec -> spec.onDemand()));
            for (int e = 0; e < BACKLOG_EXECUTIONS_PER_JOB; e++) {
                batchArgs.add(new Object[] {
                        UUID.randomUUID().toString(), jobKey, JdbcTimestamps.toUtcTimestamp(NOW.minusSeconds(1)),
                        Priority.NORMAL.value(), JdbcTimestamps.toUtcTimestamp(NOW)
                });
            }
        }
        rawJdbcTemplate.batchUpdate("""
                INSERT INTO mohs_executions (
                    id, job_key, state, scheduled_at, actor, priority, payload, payload_type, created_at)
                VALUES (?, ?, 'ENQUEUED', ?, 'harness', ?, '{}', 'java.lang.Object', ?)
                """, batchArgs);
    }

    private long indexSizeBytes(DataSource dataSource, String indexSizeSql) throws SQLException {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(indexSizeSql)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    private double measureThroughput(DataSource pool, JdbcDialect dialect, MutableClock clock) throws Exception {
        JdbcJobStore jobStore = new JdbcJobStore(pool, clock);
        JdbcExecutionStore executionStore = new JdbcExecutionStore(pool, clock, JsonMapper.builder().build());
        AtomicInteger claimedTotal = new AtomicInteger();
        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        List<Future<?>> nodes = new ArrayList<>();
        long start = System.nanoTime();
        for (int i = 0; i < CONCURRENT_NODES; i++) {
            JdbcClaimer nodeClaimer = new JdbcClaimer(pool, dialect, clock, executionStore, jobStore, LEASE_TTL, new ExecutionWindowRegistry(List.of()));
            String nodeId = "index-tuning-node-" + i;
            nodes.add(executor.submit(() -> {
                List<Execution> claimed;
                do {
                    claimed = nodeClaimer.claim(nodeId, BATCH_SIZE);
                    claimedTotal.addAndGet(claimed.size());
                } while (!claimed.isEmpty());
            }));
        }
        for (Future<?> node : nodes) {
            node.get(120, TimeUnit.SECONDS);
        }
        long elapsedNanos = System.nanoTime() - start;
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        double elapsedSeconds = elapsedNanos / 1_000_000_000.0;
        return claimedTotal.get() / elapsedSeconds;
    }

    private void printReport(DialectResult r) {
        System.out.println();
        System.out.println("=== ClaimIndexTuningHarness — " + r.dialect() + " — full vs. filtered idx_mohs_executions_claim, "
                + HISTORY_ROWS + " linhas de historico ===");
        System.out.printf("%-12s %16s %16s %16s %18s %18s%n",
                "Dialeto", "size antes (KB)", "size depois (KB)", "reducao", "throughput antes", "throughput depois");
        System.out.printf("%-12s %16.1f %16.1f %15.1f%% %18.1f %18.1f%n",
                r.dialect(), r.sizeBeforeBytes() / 1024.0, r.sizeAfterBytes() / 1024.0,
                100.0 * (1.0 - (double) r.sizeAfterBytes() / r.sizeBeforeBytes()),
                r.throughputBefore(), r.throughputAfter());
        System.out.println();
    }

    record DialectResult(String dialect, long sizeBeforeBytes, long sizeAfterBytes, double throughputBefore, double throughputAfter) {
    }
}
