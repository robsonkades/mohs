package io.mohs.store.jdbc;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
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
import java.util.stream.IntStream;

import javax.sql.DataSource;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

import io.mohs.core.definition.JobDefinition;
import io.mohs.test.MutableClock;

/**
 * DBTUNE-15 (investigação, não conclusão): {@code IN (:ids)} vs. {@code
 * JOIN} contra uma lista {@code VALUES} derivada, nos 4 dialetos, pro
 * ponto de maior risco entre os 4 lugares que usam {@code IN} em produção
 * — {@link JdbcExecutionStore#findByIds}, o único dos 4 que está no hot
 * path do claim ({@code JdbcClaimer.claim} chama isto toda vez).
 *
 * <p>Mesma disciplina exigida pelo usuário pra qualquer conversa de
 * performance neste projeto: simulação real (Testcontainers, os 4
 * bancos) + plano de execução (não só throughput) — nunca conclusão sem
 * medir. Histórico de 20k linhas (mesmo truque do {@code
 * ClaimIndexTuningHarness}/{@code LivenessLoadHarness}) pra não medir
 * contra uma tabela vazia, onde os dois planos são igualmente triviais.
 *
 * <p>Escopo desta rodada: só {@code findByIds}. Os outros 3 pontos
 * (attempts em lote, {@code completeAll}) ficam pra uma rodada seguinte
 * se este primeiro resultado justificar o esforço — ver {@code
 * docs/adr} não se aplica aqui, é investigação, não decisão de
 * arquitetura ainda.
 *
 * <p>Separado da suíte unitária: {@code mvn test
 * -Dtest=InVsJoinTuningHarness}.
 */
class InVsJoinTuningHarness {

    record Handler() {
    }

    private static final Instant NOW = Instant.parse("2026-08-14T12:00:00Z");
    private static final int HISTORY_ROWS = 20_000;
    private static final int HISTORY_JOB_COUNT = 500;
    private static final int HISTORY_BATCH_CHUNK = 2_000;

    /** Mesmo batchSize do claim real (BATCH_SIZE em JdbcClaimer/ClaimQueryLoadHarness). */
    private static final int LOOKUP_BATCH_SIZE = 20;
    private static final int WARMUP = 10;
    private static final int SAMPLES = 50;

    private static final Path OUTPUT_DIR = Path.of("docs", "performance");
    private static final String EXECUTION_COLUMNS = "id, job_key, state, scheduled_at, fired_at, actor";

    @Test
    void findByIdsInVsJoin() throws Exception {
        Files.createDirectories(OUTPUT_DIR);
        List<Result> results = new ArrayList<>();
        results.add(measure("H2", "h2", InVsJoinTuningHarness::freshH2DataSource, false));
        results.add(measure("PostgreSQL", "postgresql", PostgresTestSupport::freshSchema, false));
        results.add(measure("MySQL", "mysql", MySqlTestSupport::freshSchema, true));
        results.add(measure("SQL Server", "sqlserver", SqlServerTestSupport::freshSchema, false));
        printReport(results);
    }

    private Result measure(String label, String fileLabel, Supplier<DataSource> rawDataSourceFactory, boolean mysqlRowConstructor) throws Exception {
        DataSource raw = rawDataSourceFactory.get();
        MutableClock clock = new MutableClock(NOW, ZoneId.of("UTC"));
        List<String> ids;
        try (HikariDataSource pool = pooledDataSource(raw)) {
            seedHistory(pool, clock);
            ids = seedLookupBatch(pool, clock);
        }

        LatencyStats inStats;
        List<String> inExplain;
        try (Connection connection = raw.getConnection()) {
            inExplain = explainIn(connection, ids);
            inStats = measureLatency(connection, ids, this::runInQuery);
        }
        writeFile("invsjoin-" + fileLabel + "-in", inExplain);

        LatencyStats joinStats;
        List<String> joinExplain;
        try (Connection connection = raw.getConnection()) {
            joinExplain = explainJoin(connection, ids, mysqlRowConstructor);
            joinStats = measureLatency(connection, ids, (c, batch) -> runJoinQuery(c, batch, mysqlRowConstructor));
        }
        writeFile("invsjoin-" + fileLabel + "-join", joinExplain);

        return new Result(label, inStats, joinStats, scanSummary(inExplain), scanSummary(joinExplain));
    }

    private interface QueryRunner {
        int run(Connection connection, List<String> ids) throws SQLException;
    }

    private LatencyStats measureLatency(Connection connection, List<String> ids, QueryRunner runner) throws SQLException {
        for (int i = 0; i < WARMUP; i++) {
            runner.run(connection, ids);
        }
        long[] samplesNanos = new long[SAMPLES];
        for (int i = 0; i < SAMPLES; i++) {
            long start = System.nanoTime();
            int rows = runner.run(connection, ids);
            samplesNanos[i] = System.nanoTime() - start;
            if (rows != ids.size()) {
                throw new IllegalStateException("expected " + ids.size() + " rows, got " + rows);
            }
        }
        return LatencyStats.of(samplesNanos);
    }

    private int runInQuery(Connection connection, List<String> ids) throws SQLException {
        String placeholders = String.join(",", ids.stream().map(_ -> "?").toList());
        String sql = "SELECT " + EXECUTION_COLUMNS + " FROM mohs_executions WHERE id IN (" + placeholders + ")";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            bindIds(statement, ids, 1);
            return countRows(statement);
        }
    }

    private int runJoinQuery(Connection connection, List<String> ids, boolean mysqlRowConstructor) throws SQLException {
        String sql = "SELECT e.id, e.job_key, e.state, e.scheduled_at, e.fired_at, e.actor "
                + "FROM mohs_executions e JOIN " + valuesJoin(ids.size(), mysqlRowConstructor) + " AS lookup_ids(id) ON e.id = lookup_ids.id";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            bindIds(statement, ids, 1);
            return countRows(statement);
        }
    }

    private static int countRows(PreparedStatement statement) throws SQLException {
        try (ResultSet rs = statement.executeQuery()) {
            int count = 0;
            while (rs.next()) {
                count++;
            }
            return count;
        }
    }

    private static void bindIds(PreparedStatement statement, List<String> ids, int startIndex) throws SQLException {
        for (int i = 0; i < ids.size(); i++) {
            statement.setString(startIndex + i, ids.get(i));
        }
    }

    /** {@code (VALUES (?),(?),...)} — MySQL 8.0.19+ table value constructor exige {@code ROW(?)} em vez de {@code (?)}. */
    private static String valuesJoin(int count, boolean mysqlRowConstructor) {
        String row = mysqlRowConstructor ? "ROW(?)" : "(?)";
        String rows = IntStream.range(0, count).mapToObj(_ -> row).collect(java.util.stream.Collectors.joining(","));
        return "(VALUES " + rows + ")";
    }

    private List<String> explainIn(Connection connection, List<String> ids) throws SQLException {
        String product = connection.getMetaData().getDatabaseProductName();
        if ("Microsoft SQL Server".equals(product)) {
            String sql = "SELECT " + EXECUTION_COLUMNS + " FROM mohs_executions WHERE id IN (" + literalIdList(ids) + ")";
            return explainSqlServer(connection, sql);
        }
        String placeholders = String.join(",", ids.stream().map(_ -> "?").toList());
        String sql = explainPrefix(product) + "SELECT " + EXECUTION_COLUMNS + " FROM mohs_executions WHERE id IN (" + placeholders + ")";
        return explainParameterized(connection, sql, ids);
    }

    private List<String> explainJoin(Connection connection, List<String> ids, boolean mysqlRowConstructor) throws SQLException {
        String product = connection.getMetaData().getDatabaseProductName();
        if ("Microsoft SQL Server".equals(product)) {
            String sql = "SELECT e.id, e.job_key, e.state, e.scheduled_at, e.fired_at, e.actor "
                    + "FROM mohs_executions e JOIN (VALUES " + literalValuesRows(ids) + ") AS lookup_ids(id) ON e.id = lookup_ids.id";
            return explainSqlServer(connection, sql);
        }
        String sql = explainPrefix(product)
                + "SELECT e.id, e.job_key, e.state, e.scheduled_at, e.fired_at, e.actor "
                + "FROM mohs_executions e JOIN " + valuesJoin(ids.size(), mysqlRowConstructor) + " AS lookup_ids(id) ON e.id = lookup_ids.id";
        return explainParameterized(connection, sql, ids);
    }

    private static String explainPrefix(String product) {
        return switch (product) {
            case "PostgreSQL" -> "EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) ";
            case "MySQL" -> "EXPLAIN ANALYZE ";
            default -> "EXPLAIN "; // H2
        };
    }

    private static String literalIdList(List<String> ids) {
        return String.join(",", ids.stream().map(id -> "'" + id + "'").toList());
    }

    private static String literalValuesRows(List<String> ids) {
        return String.join(",", ids.stream().map(id -> "('" + id + "')").toList());
    }

    private static List<String> explainParameterized(Connection connection, String sql, List<String> ids) throws SQLException {
        List<String> lines = new ArrayList<>();
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            bindIds(statement, ids, 1);
            try (ResultSet rs = statement.executeQuery()) {
                appendRows(rs, lines);
            }
        }
        return lines;
    }

    /**
     * {@code SET SHOWPLAN_ALL} exige ser o único statement do batch e não
     * convive bem com {@code PreparedStatement} (sp_prepexec) — SQL literal
     * embutido via {@code Statement} puro, mesma solução do {@code
     * ClaimQueryExplainHarness}. Seguro aqui: os ids são UUIDs gerados por
     * nós mesmos, nunca entrada externa.
     */
    private static List<String> explainSqlServer(Connection connection, String sql) throws SQLException {
        List<String> lines = new ArrayList<>();
        try (Statement showplan = connection.createStatement()) {
            showplan.execute("SET SHOWPLAN_ALL ON");
        }
        try (Statement statement = connection.createStatement(); ResultSet rs = statement.executeQuery(sql)) {
            appendRows(rs, lines);
        }
        try (Statement showplanOff = connection.createStatement()) {
            showplanOff.execute("SET SHOWPLAN_ALL OFF");
        }
        return lines;
    }

    private static void appendRows(ResultSet rs, List<String> lines) throws SQLException {
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

    private static String scanSummary(List<String> explainLines) {
        return explainLines.stream()
                .filter(line -> line.toLowerCase().contains("scan") || line.toLowerCase().contains("join") || line.toLowerCase().contains("nested"))
                .findFirst()
                .orElse(explainLines.isEmpty() ? "(sem plano)" : explainLines.get(0))
                .trim();
    }

    private HikariDataSource pooledDataSource(DataSource raw) {
        HikariConfig config = new HikariConfig();
        config.setDataSource(raw);
        config.setMaximumPoolSize(4);
        config.setConnectionTimeout(Duration.ofSeconds(2).toMillis());
        config.setPoolName("in-vs-join-harness");
        return new HikariDataSource(config);
    }

    /** Execuções terminais (peso morto) — mesmo truque do ClaimIndexTuningHarness/LivenessLoadHarness. */
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
            var at = JdbcTimestamps.toUtcLocalDateTime(NOW.minusSeconds(HISTORY_ROWS - i));
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

    /** Lote real de ids ENQUEUED — mesma forma do que {@code JdbcClaimer.claim} passaria pra {@code findByIds} depois do claim. */
    private List<String> seedLookupBatch(DataSource pool, MutableClock clock) {
        JdbcJobStore jobStore = new JdbcJobStore(pool, clock);
        JdbcTemplate rawJdbcTemplate = new JdbcTemplate(pool);
        jobStore.upsert(JobDefinition.of("lookup-job", Handler.class, spec -> spec.onDemand()));
        List<String> ids = new ArrayList<>(LOOKUP_BATCH_SIZE);
        List<Object[]> batchArgs = new ArrayList<>();
        for (int i = 0; i < LOOKUP_BATCH_SIZE; i++) {
            String id = UUID.randomUUID().toString();
            ids.add(id);
            batchArgs.add(new Object[] {
                    id, "lookup-job", JdbcTimestamps.toUtcLocalDateTime(NOW.minusSeconds(1)), JdbcTimestamps.toUtcLocalDateTime(NOW)
            });
        }
        rawJdbcTemplate.batchUpdate("""
                INSERT INTO mohs_executions (
                    id, job_key, state, scheduled_at, actor, payload, payload_type, created_at)
                VALUES (?, ?, 'ENQUEUED', ?, 'harness', '{}', 'java.lang.Object', ?)
                """, batchArgs);
        return ids;
    }

    private static DataSource freshH2DataSource() {
        JdbcDataSource h2 = new JdbcDataSource();
        h2.setURL("jdbc:h2:mem:in-vs-join-harness-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        h2.setUser("sa");
        h2.setPassword("");
        new ResourceDatabasePopulator(new ClassPathResource("schema-h2.sql")).execute(h2);
        return h2;
    }

    private void writeFile(String label, List<String> lines) throws IOException {
        Path file = OUTPUT_DIR.resolve("explain-" + label + ".txt");
        Files.write(file, lines);
        System.out.println("wrote " + file);
    }

    private void printReport(List<Result> results) {
        System.out.println();
        System.out.println("=== InVsJoinTuningHarness — findByIds: IN (:ids) vs JOIN VALUES (batchSize=" + LOOKUP_BATCH_SIZE
                + ", " + HISTORY_ROWS + " linhas de histórico) ===");
        System.out.printf("%-12s %10s %10s %10s %10s%n", "Dialeto", "IN p50(ms)", "IN p99(ms)", "JOIN p50(ms)", "JOIN p99(ms)");
        for (Result result : results) {
            System.out.printf("%-12s %10.3f %10.3f %10.3f %10.3f%n",
                    result.dialect(), result.in().p50Ms(), result.in().p99Ms(), result.join().p50Ms(), result.join().p99Ms());
            System.out.println("  plano IN:   " + result.scanIn());
            System.out.println("  plano JOIN: " + result.scanJoin());
        }
        System.out.println();
    }

    record Result(String dialect, LatencyStats in, LatencyStats join, String scanIn, String scanJoin) {
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
