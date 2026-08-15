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
import java.util.function.Function;
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
 * N3 do codereview-20260815: captura o plano de execução real das formas
 * principais da query de {@code JdbcExecutionStore#findPage} ({@code GET
 * /executions}) contra os 4 dialetos, antes de decidir qualquer índice —
 * os índices existentes servem claim/reaper, nenhum cobre {@code (state,
 * id)} ou {@code (job_key, id)}, e a regra da casa é medir antes de
 * opinar. Cenário realista de paginação: histórico terminal dominante
 * (~95% {@code SUCCEEDED}) com backlog vivo pequeno — a proporção em que
 * um filtro por state raro é caro e um por state comum é barato num scan
 * reverso da PK. Ids UUIDv7 como em produção (time-ordered — {@code ORDER
 * BY id DESC} anda na ordem de inserção, não aleatório como um v4).
 *
 * <p>Formas capturadas, todas com {@code LIMIT 51} (DEFAULT_PAGE_SIZE + a
 * sonda de próxima página): sem filtro · {@code state} comum · {@code
 * state} raro · {@code job_key} · {@code job_key} + cursor (keyset).
 *
 * <p>Conexão direta não pooled pra captura de plano — {@code SET
 * SHOWPLAN_ALL ON} (SQL Server) muda estado de sessão (mesmo cuidado de
 * {@link ClaimQueryExplainHarness}).
 *
 * <p>Separado da suíte unitária, mesmo padrão dos outros harnesses:
 * {@code mvn test -Dtest=FindPageQueryExplainHarness}.
 */
class FindPageQueryExplainHarness {

    record Handler() {
    }

    private static final Instant NOW = Instant.parse("2026-08-15T12:00:00Z");
    private static final int JOB_COUNT = 150;
    private static final int EXECUTIONS_PER_JOB = 200; // 30k linhas — histórico dominante
    private static final int PAGE_LIMIT = 51;
    private static final String FILTERED_JOB = "findpage-job-42";
    private static final Path OUTPUT_DIR = Path.of("docs", "performance");

    private static final String COLUMNS = "id, job_key, state, scheduled_at, fired_at, actor, priority, idempotency_key";

    @Test
    void run() throws Exception {
        Files.createDirectories(OUTPUT_DIR);
        captureExplain("h2", FindPageQueryExplainHarness::freshH2DataSource, "ANALYZE", "EXPLAIN ", FindPageQueryExplainHarness::ansiSelect);
        captureExplain("postgresql", PostgresTestSupport::freshSchema, "ANALYZE mohs_executions", "EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) ", FindPageQueryExplainHarness::ansiSelect);
        captureExplain("mysql", MySqlTestSupport::freshSchema, "ANALYZE TABLE mohs_executions", "EXPLAIN ANALYZE ", FindPageQueryExplainHarness::ansiSelect);
        captureShowplanSqlServer();
    }

    /**
     * {@code statsSql} atualiza as estatísticas do otimizador depois do seed
     * — sem isso o plano capturado reflete estatísticas defasadas (estimate
     * de 6 linhas contra 28.500 reais numa captura preliminar no Postgres),
     * não a condição de produção, onde autovacuum/ANALYZE mantém os números.
     */
    private void captureExplain(String label, Supplier<DataSource> dataSourceFactory, String statsSql, String explainPrefix,
            Function<String, String> select) throws SQLException, IOException {
        DataSource dataSource = dataSourceFactory.get();
        String cursorId = seedHistory(dataSource);
        new JdbcTemplate(dataSource).execute(statsSql);
        List<String> lines = new ArrayList<>();
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            for (Map.Entry<String, String> shape : shapes(select, cursorId).entrySet()) {
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

    /** {@code SET SHOWPLAN_ALL} precisa ser o único statement do batch (T-SQL) — chamadas separadas na mesma conexão; nada executa de verdade sob showplan. */
    private void captureShowplanSqlServer() throws SQLException, IOException {
        DataSource dataSource = SqlServerTestSupport.freshSchema();
        String cursorId = seedHistory(dataSource);
        // estatísticas ANTES do showplan — sob SHOWPLAN_ALL nada executa de verdade
        new JdbcTemplate(dataSource).execute("UPDATE STATISTICS mohs_executions");
        List<String> lines = new ArrayList<>();
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute("SET SHOWPLAN_ALL ON");
            for (Map.Entry<String, String> shape : shapes(FindPageQueryExplainHarness::sqlServerSelect, cursorId).entrySet()) {
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

    private static Map<String, String> shapes(Function<String, String> select, String cursorId) {
        Map<String, String> shapes = new LinkedHashMap<>();
        shapes.put("sem filtro (primeira pagina)", select.apply(""));
        shapes.put("state comum (SUCCEEDED)", select.apply(" WHERE state = 'SUCCEEDED'"));
        shapes.put("state raro (ENQUEUED)", select.apply(" WHERE state = 'ENQUEUED'"));
        shapes.put("job_key", select.apply(" WHERE job_key = '" + FILTERED_JOB + "'"));
        shapes.put("job_key + cursor (keyset)", select.apply(" WHERE job_key = '" + FILTERED_JOB + "' AND id < '" + cursorId + "'"));
        return shapes;
    }

    private static String ansiSelect(String where) {
        return "SELECT " + COLUMNS + " FROM mohs_executions" + where + " ORDER BY id DESC LIMIT " + PAGE_LIMIT;
    }

    private static String sqlServerSelect(String where) {
        return "SELECT TOP (" + PAGE_LIMIT + ") " + COLUMNS + " FROM mohs_executions" + where + " ORDER BY id DESC";
    }

    /**
     * ~95% SUCCEEDED, 3% FAILED, 1% ENQUEUED, 1% RUNNING por job — histórico
     * dominante, backlog vivo pequeno. Retorna o id "do meio" do job
     * filtrado, usado como cursor da forma keyset.
     *
     * @return id da centésima execução (de 200) de {@link #FILTERED_JOB}
     */
    private static String seedHistory(DataSource dataSource) {
        MutableClock clock = new MutableClock(NOW, ZoneId.of("UTC"));
        JdbcJobStore jobStore = new JdbcJobStore(dataSource, clock);
        JdbcTemplate rawJdbcTemplate = new JdbcTemplate(dataSource);
        String cursorId = null;
        List<Object[]> batchArgs = new ArrayList<>();
        for (int j = 0; j < JOB_COUNT; j++) {
            String jobKey = "findpage-job-" + j;
            jobStore.upsert(JobDefinition.of(jobKey, Handler.class, spec -> spec.onDemand()));
            for (int e = 0; e < EXECUTIONS_PER_JOB; e++) {
                String id = UUIDv7.randomUUIDString();
                if (jobKey.equals(FILTERED_JOB) && e == EXECUTIONS_PER_JOB / 2) {
                    cursorId = id;
                }
                String state = switch (e % 100) {
                    case 0 -> "ENQUEUED";
                    case 1 -> "RUNNING";
                    case 2, 3, 4 -> "FAILED";
                    default -> "SUCCEEDED";
                };
                batchArgs.add(new Object[] {
                        id, jobKey, state, JdbcTimestamps.toUtcTimestamp(NOW.minusSeconds(EXECUTIONS_PER_JOB - e)),
                        Priority.NORMAL.value(), JdbcTimestamps.toUtcTimestamp(NOW)
                });
            }
        }
        rawJdbcTemplate.batchUpdate("""
                INSERT INTO mohs_executions (
                    id, job_key, state, scheduled_at, actor, priority, payload, payload_type, created_at)
                VALUES (?, ?, ?, ?, 'findpage-harness', ?, '{}', 'java.lang.Object', ?)
                """, batchArgs);
        return cursorId;
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
        h2.setURL("jdbc:h2:mem:findpage-explain-harness-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        h2.setUser("sa");
        h2.setPassword("");
        new ResourceDatabasePopulator(new ClassPathResource("schema-h2.sql")).execute(h2);
        return h2;
    }

    private void writeFile(String label, List<String> lines) throws IOException {
        Path file = OUTPUT_DIR.resolve("explain-findpage-" + label + ".txt");
        Files.write(file, lines);
        System.out.println("wrote " + file);
    }
}
