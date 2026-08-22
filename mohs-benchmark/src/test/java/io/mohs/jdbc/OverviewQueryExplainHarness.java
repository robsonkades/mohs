package io.mohs.jdbc;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;

import javax.sql.DataSource;

import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

import io.github.robsonkades.uuidv7.UUIDv7;

import io.mohs.core.definition.JobDefinition;
import io.mohs.core.execution.Priority;
import io.mohs.test.MutableClock;

/**
 * Plano de execução real das três queries de {@code GET /overview}
 * ({@code JdbcExecutionStore#countActiveByState}/{@code
 * countTerminalOutcomesSince}) contra os 4 dialetos — a promessa do
 * endpoint é "barato por construção" (custo proporcional ao trabalho vivo
 * e à janela, nunca ao histórico), e promessa de performance sem plano
 * capturado é opinião (regra da casa). Cenário: histórico terminal
 * dominante (~99% das linhas) com backlog vivo pequeno e attempts
 * espalhados por ~28h, dos quais só a última janela de 60s interessa —
 * exatamente a proporção em que um plano errado degrada pra scan da
 * tabela inteira a cada poll do dashboard.
 *
 * <p>O que cada plano tem que mostrar: backlog pelo índice parcial de
 * claim (o predicado {@code IN (ENQUEUED, RETRY_SCHEDULED)} é o do
 * índice); {@code RUNNING} pelo índice parcial do reaper (o {@code
 * lease_expires_at IS NOT NULL} está lá só pra isso — DBTUNE-17); janela
 * pelo range curto de {@code idx_mohs_attempts_throughput}.
 *
 * <p>Conexão direta não pooled pra captura de plano — {@code SET
 * SHOWPLAN_ALL ON} (SQL Server) muda estado de sessão (mesmo cuidado de
 * {@link ClaimQueryExplainHarness}). Separado da suíte unitária, mesmo
 * padrão dos outros harnesses: {@code mvn test
 * -Dtest=OverviewQueryExplainHarness} (ou um dialeto só, como {@code
 * ClaimIndexTuningHarness}: {@code -Dtest=OverviewQueryExplainHarness#sqlServer}
 * — um container que falhar não custa a rodada dos demais).
 */
class OverviewQueryExplainHarness {

    record Handler() {
    }

    private static final Instant NOW = Instant.parse("2026-08-17T12:00:00Z");
    private static final int JOB_COUNT = 10;
    private static final int TERMINAL_COUNT = 100_000;
    private static final int ENQUEUED_COUNT = 500;
    private static final int RUNNING_COUNT = 100;
    private static final int RETRY_SCHEDULED_COUNT = 50;
    private static final Path OUTPUT_DIR = Path.of("docs", "performance");

    @Test
    void h2() throws Exception {
        captureExplain("h2", OverviewQueryExplainHarness::freshH2DataSource,
                List.of("ANALYZE"), "EXPLAIN ", "TIMESTAMP '2026-08-17 11:59:00'");
    }

    @Test
    void postgres() throws Exception {
        captureExplain("postgresql", PostgresTestSupport::freshSchema,
                List.of("ANALYZE mohs_executions", "ANALYZE mohs_attempts"),
                "EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) ", "TIMESTAMP '2026-08-17 11:59:00'");
    }

    @Test
    void mySql() throws Exception {
        captureExplain("mysql", MySqlTestSupport::freshSchema,
                List.of("ANALYZE TABLE mohs_executions", "ANALYZE TABLE mohs_attempts"),
                "EXPLAIN ANALYZE ", "TIMESTAMP '2026-08-17 11:59:00'");
    }

    @Test
    void sqlServer() throws Exception {
        captureShowplanSqlServer();
    }

    /**
     * As três queries de produção, com o parâmetro da janela inlinado como
     * literal — captura de plano não usa bind. {@code lockHint} espelha
     * {@code JdbcDialect#lockFreeCountHint}: vazio nos MVCC, {@code WITH
     * (NOLOCK)} no SQL Server — o SQL capturado tem que ser o SQL que roda.
     */
    private static Map<String, String> shapes(String sinceLiteral, String lockHint) {
        Map<String, String> shapes = new LinkedHashMap<>();
        shapes.put("backlog (ENQUEUED + RETRY_SCHEDULED, indice parcial de claim)",
                "SELECT state, COUNT(*) AS total FROM mohs_executions " + lockHint
                        + "WHERE state IN ('ENQUEUED', 'RETRY_SCHEDULED') GROUP BY state");
        shapes.put("RUNNING (indice parcial do reaper via IS NOT NULL)",
                "SELECT COUNT(*) FROM mohs_executions " + lockHint
                        + "WHERE state = 'RUNNING' AND lease_expires_at IS NOT NULL");
        shapes.put("janela de vazao (idx_mohs_attempts_throughput)",
                "SELECT outcome, COUNT(*) AS total FROM mohs_attempts " + lockHint
                        + "WHERE finished_at >= " + sinceLiteral
                        + " AND outcome IN ('SUCCEEDED', 'FAILED') GROUP BY outcome");
        return shapes;
    }

    /** {@code statsSql} atualiza as estatísticas do otimizador depois do seed — mesmo racional de {@link FindPageQueryExplainHarness}. */
    private void captureExplain(String label, Supplier<DataSource> dataSourceFactory, List<String> statsSql,
            String explainPrefix, String sinceLiteral) throws SQLException, IOException {
        DataSource dataSource = dataSourceFactory.get();
        seed(dataSource);
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        statsSql.forEach(jdbcTemplate::execute);
        List<String> lines = new ArrayList<>();
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            for (Map.Entry<String, String> shape : shapes(sinceLiteral, "").entrySet()) {
                lines.add("=== " + shape.getKey() + " ===");
                lines.add(shape.getValue());
                lines.add("");
                try (ResultSet rs = statement.executeQuery(explainPrefix + shape.getValue())) {
                    appendRows(rs, lines);
                }
                lines.add("");
            }
        }
        writeFile(label, lines);
    }

    /** {@code SET SHOWPLAN_ALL} precisa ser o único statement do batch (T-SQL) — mesmo cuidado de {@link FindPageQueryExplainHarness}. */
    private void captureShowplanSqlServer() throws SQLException, IOException {
        DataSource dataSource = SqlServerTestSupport.freshSchema();
        seed(dataSource);
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        jdbcTemplate.execute("UPDATE STATISTICS mohs_executions");
        jdbcTemplate.execute("UPDATE STATISTICS mohs_attempts");
        List<String> lines = new ArrayList<>();
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute("SET SHOWPLAN_ALL ON");
            for (Map.Entry<String, String> shape : shapes("'2026-08-17T11:59:00'", "WITH (NOLOCK) ").entrySet()) {
                lines.add("=== " + shape.getKey() + " ===");
                lines.add(shape.getValue());
                lines.add("");
                try (ResultSet rs = statement.executeQuery(shape.getValue())) {
                    appendRows(rs, lines);
                }
                lines.add("");
            }
        }
        writeFile("sqlserver", lines);
    }

    /**
     * ~99% terminal (95% SUCCEEDED / 5% FAILED), cada um com seu attempt —
     * {@code finished_at} espalhado 1s por linha (~28h de histórico), então
     * a janela de 60s cobre só as últimas ~60 linhas: a diferença entre um
     * range scan e um scan do histórico aparece no plano. Backlog vivo
     * pequeno por cima; {@code RUNNING} com lease e node, como o claim
     * deixa.
     */
    private static void seed(DataSource dataSource) {
        MutableClock clock = new MutableClock(NOW, ZoneId.of("UTC"));
        JdbcJobStore jobStore = new JdbcJobStore(dataSource, clock);
        for (int j = 0; j < JOB_COUNT; j++) {
            jobStore.upsert(JobDefinition.of("overview-job-" + j, Handler.class, spec -> spec.onDemand()));
        }
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);

        List<Object[]> executions = new ArrayList<>();
        List<Object[]> attempts = new ArrayList<>();
        for (int i = 0; i < TERMINAL_COUNT; i++) {
            String id = UUIDv7.randomUUIDString();
            String outcome = i % 20 == 0 ? "FAILED" : "SUCCEEDED";
            Instant finishedAt = NOW.minusSeconds(i);
            executions.add(executionRow(id, "overview-job-" + (i % JOB_COUNT), outcome, finishedAt.minusSeconds(1)));
            attempts.add(new Object[] {
                    id, 1,
                    JdbcTimestamps.toUtcTimestamp(finishedAt.minusSeconds(1)),
                    JdbcTimestamps.toUtcTimestamp(finishedAt),
                    outcome
            });
        }
        for (int i = 0; i < ENQUEUED_COUNT; i++) {
            executions.add(executionRow(UUIDv7.randomUUIDString(), "overview-job-" + (i % JOB_COUNT), "ENQUEUED", NOW));
        }
        for (int i = 0; i < RETRY_SCHEDULED_COUNT; i++) {
            executions.add(executionRow(UUIDv7.randomUUIDString(), "overview-job-" + (i % JOB_COUNT), "RETRY_SCHEDULED", NOW));
        }
        jdbcTemplate.batchUpdate("""
                INSERT INTO mohs_executions (
                    id, job_key, state, scheduled_at, actor, priority, payload, payload_type, created_at)
                VALUES (?, ?, ?, ?, 'overview-harness', ?, '{}', 'java.lang.Object', ?)
                """, executions);
        List<Object[]> running = new ArrayList<>();
        for (int i = 0; i < RUNNING_COUNT; i++) {
            running.add(new Object[] {
                    UUIDv7.randomUUIDString(), "overview-job-" + (i % JOB_COUNT),
                    JdbcTimestamps.toUtcTimestamp(NOW), Priority.NORMAL.value(),
                    JdbcTimestamps.toUtcTimestamp(NOW.plusSeconds(30)),
                    JdbcTimestamps.toUtcTimestamp(NOW)
            });
        }
        jdbcTemplate.batchUpdate("""
                INSERT INTO mohs_executions (
                    id, job_key, state, scheduled_at, actor, priority, node_id, lease_expires_at, payload, payload_type, created_at)
                VALUES (?, ?, 'RUNNING', ?, 'overview-harness', ?, 'node-harness', ?, '{}', 'java.lang.Object', ?)
                """, running);
        jdbcTemplate.batchUpdate("""
                INSERT INTO mohs_attempts (execution_id, number, started_at, finished_at, outcome)
                VALUES (?, ?, ?, ?, ?)
                """, attempts);
    }

    private static Object[] executionRow(String id, String jobKey, String state, Instant scheduledAt) {
        return new Object[] {
                id, jobKey, state,
                JdbcTimestamps.toUtcTimestamp(scheduledAt), Priority.NORMAL.value(),
                JdbcTimestamps.toUtcTimestamp(NOW)
        };
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

    private static DataSource freshH2DataSource() {
        JdbcDataSource h2 = new JdbcDataSource();
        h2.setURL("jdbc:h2:mem:overview-explain-harness-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        h2.setUser("sa");
        h2.setPassword("");
        new ResourceDatabasePopulator(new ClassPathResource("schema-h2.sql")).execute(h2);
        return h2;
    }

    private void writeFile(String label, List<String> lines) throws IOException {
        Files.createDirectories(OUTPUT_DIR);
        Path file = OUTPUT_DIR.resolve("explain-overview-" + label + ".txt");
        Files.write(file, lines);
        System.out.println("wrote " + file);
    }
}
