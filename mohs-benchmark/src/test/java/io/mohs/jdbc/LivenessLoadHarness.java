package io.mohs.jdbc;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;

import javax.sql.DataSource;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

import tools.jackson.databind.json.JsonMapper;

import io.mohs.core.EngineState;
import io.mohs.core.definition.JobDefinition;
import io.mohs.core.execution.Execution;
import io.mohs.engine.Reaper;
import io.mohs.jdbc.dialect.H2JdbcDialect;
import io.mohs.jdbc.dialect.JdbcDialect;
import io.mohs.jdbc.dialect.PostgresJdbcDialect;
import io.mohs.test.MutableClock;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Harness de carga de {@code NodeStore.heartbeat}/{@code
 * Reaper.reclaimExpired} — caráter informativo, não gate de tuning
 * (CLAUDE.md: "sem número, não é otimização" vale pra evitar reivindicar
 * ganho sem medir; a parte de {@code heartbeat} não reivindica ganho
 * nenhum, só caracteriza um caminho deliberadamente não-hot-path). A
 * parte de {@code reclaimExpired} mede de verdade o índice DBTUNE-10
 * (antes/depois, mesmo container) — sem histórico "de peso morto" na
 * tabela, table scan e index scan custam o mesmo, então o teste enche
 * {@code mohs_executions} de execuções terminais primeiro, mesma lição
 * do {@code ClaimIndexTuningHarness} (DBTUNE-5).
 *
 * <p>Só H2 e PostgreSQL — desproporcional dar aos 4 dialetos o mesmo
 * tratamento do {@link ClaimQueryLoadHarness} para um caminho que só roda
 * sob falha de nó, não a cada ciclo. Separado da suíte unitária (nome não
 * bate {@code **&#47;*Test.java}): {@code mvn test
 * -Dtest=LivenessLoadHarness}.
 */
class LivenessLoadHarness {

    record Handler() {
    }

    private static final Instant NOW = Instant.parse("2026-08-14T12:00:00Z");

    private static final int HEARTBEAT_WARMUP = 10;
    private static final int HEARTBEAT_SAMPLES = 50;

    /** Execuções RUNNING órfãs, uma por job — isola o custo do reclaim do mutex de concorrência por job. */
    private static final int RECLAIM_BACKLOG_SIZE = 100;

    private static final int HISTORY_ROWS = 20_000;
    private static final int HISTORY_JOB_COUNT = 500;
    private static final int HISTORY_BATCH_CHUNK = 2_000;

    private static final Path OUTPUT_DIR = Path.of("docs", "performance");

    private static final String RECLAIM_QUERY_WHERE = "WHERE state = 'RUNNING' AND lease_expires_at < TIMESTAMP '2026-08-14 12:00:00'";
    private static final String H2_EXPLAIN_SQL = "EXPLAIN SELECT id, job_key FROM mohs_executions " + RECLAIM_QUERY_WHERE;
    private static final String POSTGRES_EXPLAIN_SQL = "EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) SELECT id, job_key FROM mohs_executions " + RECLAIM_QUERY_WHERE;

    @Test
    void run() throws Exception {
        Files.createDirectories(OUTPUT_DIR);
        List<HeartbeatResult> heartbeatResults = new ArrayList<>();
        heartbeatResults.add(measureHeartbeat("H2", LivenessLoadHarness::freshH2DataSource));
        heartbeatResults.add(measureHeartbeat("PostgreSQL", PostgresTestSupport::freshSchema));
        printHeartbeatReport(heartbeatResults);

        List<ReclaimResult> reclaimResults = new ArrayList<>();
        reclaimResults.add(measureReclaimBeforeAfter("H2", LivenessLoadHarness::freshH2DataSource,
                "DROP INDEX idx_mohs_executions_reaper",
                "CREATE INDEX idx_mohs_executions_reaper ON mohs_executions (state, lease_expires_at)",
                H2_EXPLAIN_SQL));
        reclaimResults.add(measureReclaimBeforeAfter("PostgreSQL", PostgresTestSupport::freshSchema,
                "DROP INDEX idx_mohs_executions_reaper",
                "CREATE INDEX idx_mohs_executions_reaper ON mohs_executions (lease_expires_at) WHERE state = 'RUNNING'",
                POSTGRES_EXPLAIN_SQL));
        printReclaimReport(reclaimResults);
    }

    private HikariDataSource pooledDataSource(DataSource raw) {
        HikariConfig config = new HikariConfig();
        config.setDataSource(raw);
        config.setMaximumPoolSize(4);
        config.setConnectionTimeout(Duration.ofSeconds(2).toMillis());
        config.setPoolName("liveness-load-harness");
        return new HikariDataSource(config);
    }

    private HeartbeatResult measureHeartbeat(String label, Supplier<DataSource> rawDataSourceFactory) throws Exception {
        try (HikariDataSource pool = pooledDataSource(rawDataSourceFactory.get())) {
            JdbcNodeStore nodeStore = new JdbcNodeStore(pool);
            for (int i = 0; i < HEARTBEAT_WARMUP; i++) {
                nodeStore.heartbeat("warmup-node", EngineState.RUNNING, NOW.plusSeconds(i));
            }
            long[] samplesNanos = new long[HEARTBEAT_SAMPLES];
            for (int i = 0; i < HEARTBEAT_SAMPLES; i++) {
                long start = System.nanoTime();
                nodeStore.heartbeat("liveness-node", EngineState.RUNNING, NOW.plusSeconds(i));
                samplesNanos[i] = System.nanoTime() - start;
            }
            return new HeartbeatResult(label, LatencyStats.of(samplesNanos));
        }
    }

    /**
     * Mesmo desenho do {@code ClaimIndexTuningHarness} (DBTUNE-5): histórico
     * de execuções terminais primeiro (peso morto que o table scan sem
     * índice paga e o index scan não), backlog RUNNING órfão medido, índice
     * trocado no meio do mesmo container, backlog reseedado, medido de novo.
     * Captura o plano de execução real da query do reaper em cada fase
     * (feedback do usuário: número sem plano não basta) — grava em {@code
     * docs/performance/explain-liveness-reaper-<label>-{before,after}.txt}.
     */
    private ReclaimResult measureReclaimBeforeAfter(String label, Supplier<DataSource> rawDataSourceFactory,
            String dropIndexSql, String createIndexSql, String explainSql) throws Exception {
        DataSource raw = rawDataSourceFactory.get();
        MutableClock clock = new MutableClock(NOW, ZoneId.of("UTC"));
        JdbcDialect dialect = "PostgreSQL".equals(label) ? new PostgresJdbcDialect() : new H2JdbcDialect();
        try (HikariDataSource pool = pooledDataSource(raw)) {
            seedHistory(pool, clock);

            // schema já cria o índice (DBTUNE-10) — derruba primeiro pra medir
            // o baseline sem ele, depois recria pra medir o ganho de verdade.
            try (Connection connection = raw.getConnection(); Statement statement = connection.createStatement()) {
                statement.execute(dropIndexSql);
            }
            seedExpiredRunningBacklog(new JdbcTemplate(pool), new JdbcJobStore(pool, clock), "before");
            List<String> explainBefore = explain(raw, explainSql);
            writeFile("liveness-reaper-" + label.toLowerCase() + "-before", explainBefore);
            double throughputBefore = runReclaim(pool, clock, dialect);

            try (Connection connection = raw.getConnection(); Statement statement = connection.createStatement()) {
                statement.execute(createIndexSql);
            }
            seedExpiredRunningBacklog(new JdbcTemplate(pool), new JdbcJobStore(pool, clock), "after");
            List<String> explainAfter = explain(raw, explainSql);
            writeFile("liveness-reaper-" + label.toLowerCase() + "-after", explainAfter);
            double throughputAfter = runReclaim(pool, clock, dialect);

            return new ReclaimResult(label, throughputBefore, throughputAfter,
                    scanSummary(explainBefore), scanSummary(explainAfter));
        }
    }

    private double runReclaim(DataSource dataSource, MutableClock clock, JdbcDialect dialect) {
        JdbcExecutionStore executionStore = new JdbcExecutionStore(dataSource, clock, JsonMapper.builder().build(), dialect);
        JdbcJobStore jobStore = new JdbcJobStore(dataSource, clock);
        JdbcReaper reaper = new JdbcReaper(dataSource, dialect, clock, executionStore, jobStore);

        long start = System.nanoTime();
        List<Reaper.Reclaimed> reclaimed = reaper.reclaimExpired();
        long elapsedNanos = System.nanoTime() - start;

        assertThat(reclaimed).hasSize(RECLAIM_BACKLOG_SIZE);
        double elapsedSeconds = elapsedNanos / 1_000_000_000.0;
        return reclaimed.size() / elapsedSeconds;
    }

    private static List<String> explain(DataSource dataSource, String explainSql) throws SQLException {
        List<String> lines = new ArrayList<>();
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(explainSql)) {
            int columns = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                StringBuilder line = new StringBuilder();
                for (int c = 1; c <= columns; c++) {
                    if (c > 1) {
                        line.append(" | ");
                    }
                    line.append(rs.getString(c));
                }
                lines.add(line.toString());
            }
        }
        return lines;
    }

    /** Primeira linha com "scan" no plano — o suficiente pra distinguir table scan de index scan/seek no resumo do console. */
    private static String scanSummary(List<String> explainLines) {
        return explainLines.stream()
                .filter(line -> line.toLowerCase().contains("scan"))
                .findFirst()
                .orElse(explainLines.isEmpty() ? "(sem plano)" : explainLines.get(0))
                .trim();
    }

    private void writeFile(String label, List<String> lines) throws IOException {
        Path file = OUTPUT_DIR.resolve("explain-" + label + ".txt");
        Files.write(file, lines);
        System.out.println("wrote " + file);
    }

    private void seedExpiredRunningBacklog(JdbcTemplate rawJdbcTemplate, JdbcJobStore jobStore, String prefix) {
        List<Object[]> batchArgs = new ArrayList<>();
        for (int j = 0; j < RECLAIM_BACKLOG_SIZE; j++) {
            String jobKey = prefix + "-reclaim-job-" + j;
            jobStore.upsert(JobDefinition.of(jobKey, Handler.class, spec -> spec.onDemand()));
            batchArgs.add(new Object[] {
                    UUID.randomUUID().toString(), jobKey,
                    JdbcTimestamps.toUtcTimestamp(NOW.minusSeconds(120)), "node-dead",
                    JdbcTimestamps.toUtcTimestamp(NOW.minusSeconds(60)),
                    JdbcTimestamps.toUtcTimestamp(NOW.minusSeconds(120))
            });
        }
        rawJdbcTemplate.batchUpdate("""
                INSERT INTO mohs_executions (
                    id, job_key, state, scheduled_at, actor, node_id, lease_expires_at, payload, payload_type, created_at)
                VALUES (?, ?, 'RUNNING', ?, 'harness', ?, ?, '{}', 'java.lang.Object', ?)
                """, batchArgs);
    }

    /** Execuções terminais (nunca RUNNING) — peso morto que só o índice sem filtro por state carregaria. */
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
            var at = JdbcTimestamps.toUtcTimestamp(NOW.minusSeconds(HISTORY_ROWS - i));
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

    private static DataSource freshH2DataSource() {
        JdbcDataSource h2 = new JdbcDataSource();
        h2.setURL("jdbc:h2:mem:liveness-load-harness-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        h2.setUser("sa");
        h2.setPassword("");
        new ResourceDatabasePopulator(new ClassPathResource("schema-h2.sql")).execute(h2);
        return h2;
    }

    private void printHeartbeatReport(List<HeartbeatResult> results) {
        System.out.println();
        System.out.println("=== LivenessLoadHarness — NodeStore.heartbeat ===");
        System.out.printf("%-12s %11s %11s %11s %11s%n", "Dialeto", "min(ms)", "p50(ms)", "p99(ms)", "max(ms)");
        for (HeartbeatResult result : results) {
            System.out.printf("%-12s %11.2f %11.2f %11.2f %11.2f%n",
                    result.dialect(), result.latency().minMs(), result.latency().p50Ms(),
                    result.latency().p99Ms(), result.latency().maxMs());
        }
        System.out.println();
    }

    private void printReclaimReport(List<ReclaimResult> results) {
        System.out.println("=== LivenessLoadHarness — Reaper.reclaimExpired (" + RECLAIM_BACKLOG_SIZE
                + " execuções órfãs, " + HISTORY_ROWS + " linhas de histórico), idx_mohs_executions_reaper (DBTUNE-10) ===");
        System.out.printf("%-12s %20s %20s%n", "Dialeto", "sem índice (rows/s)", "com índice (rows/s)");
        for (ReclaimResult result : results) {
            System.out.printf("%-12s %20.1f %20.1f%n", result.dialect(), result.throughputBefore(), result.throughputAfter());
            System.out.println("  plano antes: " + result.scanBefore());
            System.out.println("  plano depois: " + result.scanAfter());
        }
        System.out.println();
    }

    record HeartbeatResult(String dialect, LatencyStats latency) {
    }

    record ReclaimResult(String dialect, double throughputBefore, double throughputAfter, String scanBefore, String scanAfter) {
    }

    record LatencyStats(double minMs, double p50Ms, double p99Ms, double maxMs) {

        static LatencyStats of(long[] samplesNanos) {
            long[] sorted = samplesNanos.clone();
            Arrays.sort(sorted);
            return new LatencyStats(
                    toMillis(sorted[0]),
                    toMillis(sorted[sorted.length / 2]),
                    toMillis(sorted[(int) (sorted.length * 0.99)]),
                    toMillis(sorted[sorted.length - 1]));
        }

        private static double toMillis(long nanos) {
            return nanos / 1_000_000.0;
        }
    }
}
