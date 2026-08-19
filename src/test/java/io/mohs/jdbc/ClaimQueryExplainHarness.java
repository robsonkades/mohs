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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

import tools.jackson.databind.json.JsonMapper;

import io.mohs.core.definition.JobDefinition;
import io.mohs.core.execution.Execution;
import io.mohs.core.execution.Priority;
import io.mohs.engine.ExecutionWindowRegistry;
import io.mohs.jdbc.dialect.JdbcDialect;
import io.mohs.jdbc.dialect.MySqlJdbcDialect;
import io.mohs.jdbc.dialect.PostgresJdbcDialect;
import io.mohs.test.MutableClock;

/**
 * Captura o plano de execução real da claim query (ADR-0022/0023) contra
 * os 4 dialetos, na mesma escala do cenário de throughput de {@link
 * ClaimQueryLoadHarness} (300 jobs, 3000 execuções) — os planos devem
 * refletir exatamente a condição sob a qual os números de
 * {@code docs/performance/BASELINE.md} foram medidos, não uma tabela
 * vazia. Investiga também a causa do throughput de MySQL ser ~5x pior
 * que Postgres apesar de latência por chamada parecida (achado da
 * primeira rodada de {@code BASELINE.md}): mede o delta de {@code
 * Innodb_row_lock_*} (contadores globais do InnoDB, sempre disponíveis,
 * sem precisar habilitar monitor) antes/depois do mesmo cenário de
 * throughput de 8 nós.
 *
 * <p>Conexão direta por {@code DataSource} não pooled pra cada captura
 * de plano — {@code SET SHOWPLAN_ALL ON} (SQL Server) muda estado de
 * sessão; se saísse de um pool Hikari devolvido sem reset explícito,
 * corromperia a próxima query real que pegasse essa conexão de volta.
 * Só o cenário concorrente de investigação de lock usa pool (precisa de
 * 8 conexões simultâneas).
 *
 * <p>Separado da suíte unitária, mesmo padrão de {@link
 * ClaimQueryLoadHarness}: {@code mvn test -Dtest=ClaimQueryExplainHarness}.
 */
class ClaimQueryExplainHarness {

    record Handler() {
    }

    private static final Instant NOW = Instant.parse("2026-08-14T12:00:00Z");
    private static final Duration LEASE_TTL = Duration.ofSeconds(30);
    private static final int BATCH_SIZE = 20;
    private static final int JOB_COUNT = 300;
    private static final int EXECUTIONS_PER_JOB = 10; // 3000 linhas — mesma escala do cenário de throughput
    private static final int CONCURRENT_NODES = 8;
    private static final Path OUTPUT_DIR = Path.of("docs", "performance");

    /**
     * A query ANSI real com literais no lugar dos binds (EXPLAIN precisa de
     * literais) — derivada da constante, nunca copiada: drift já aconteceu
     * duas vezes (ADR-0033 ampliou o predicado de estado, b09d0b9 adicionou
     * {@code j.retired}, e as cópias desta classe ficaram para trás nas
     * duas, capturando plano de query que não existia mais). O literal
     * {@code TIMESTAMP '...'} é aceito por H2, Postgres e MySQL.
     */
    private static final String ANSI_LITERAL_SQL = JdbcDialect.ANSI_SKIP_LOCKED_CANDIDATES
            .replace(":now", "TIMESTAMP '2026-08-14 12:00:00'")
            .replace(":batchSize", String.valueOf(BATCH_SIZE));

    private static final String H2_EXPLAIN_SQL = "EXPLAIN " + ANSI_LITERAL_SQL;

    private static final String POSTGRES_EXPLAIN_SQL = "EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)\n" + ANSI_LITERAL_SQL;

    private static final String MYSQL_EXPLAIN_SQL = "EXPLAIN ANALYZE\n" + ANSI_LITERAL_SQL;

    private static final String SQLSERVER_SHOWPLAN_ON = "SET SHOWPLAN_ALL ON";

    // Única cópia manual restante — o template do SQL Server vive inline em
    // SqlServerJdbcDialect.selectCandidates, sem constante pra derivar; se
    // ele mudar, esta cópia muda junto.
    private static final String SQLSERVER_SELECT_SQL = """
            SELECT TOP (20) e.id AS id, e.job_key AS job_key,
                   j.allow_concurrent_executions AS allow_concurrent_executions,
                   j.window_name AS window_name,
                   j.rate_limit AS rate_limit
            FROM mohs_executions e WITH (UPDLOCK, ROWLOCK, READPAST)
            JOIN mohs_job_definitions j ON j.job_key = e.job_key
            WHERE e.state IN ('ENQUEUED', 'RETRY_SCHEDULED')
              AND e.scheduled_at <= '2026-08-14T12:00:00'
              AND j.retired = 0
              AND (j.allow_concurrent_executions = 1 OR j.running_execution_count < j.max_concurrent_executions)
            ORDER BY e.priority ASC, e.scheduled_at ASC
            """;

    @Test
    void run() throws Exception {
        Files.createDirectories(OUTPUT_DIR);
        // ao contrário do findPage (single-table), a claim query faz join com
        // mohs_job_definitions — as estatísticas das DUAS tabelas entram no plano
        captureExplain("h2", ClaimQueryExplainHarness::freshH2DataSource, "ANALYZE", H2_EXPLAIN_SQL);
        captureExplain("postgresql", PostgresTestSupport::freshSchema, "ANALYZE mohs_executions, mohs_job_definitions", POSTGRES_EXPLAIN_SQL);
        capturePostgresBatchCasExplain();
        captureExplain("mysql", MySqlTestSupport::freshSchema, "ANALYZE TABLE mohs_executions, mohs_job_definitions", MYSQL_EXPLAIN_SQL);
        captureShowplanSqlServer();
        investigateMySqlRowLockContention();
    }

    /** {@code statsSql} atualiza as estatísticas do otimizador depois do seed — mesma lição do {@link FindPageQueryExplainHarness}: sem isso o plano reflete estatísticas defasadas, não a condição de produção. */
    private void captureExplain(String label, Supplier<DataSource> dataSourceFactory, String statsSql, String explainSql) throws SQLException, IOException {
        DataSource dataSource = dataSourceFactory.get();
        seedBacklog(dataSource);
        new JdbcTemplate(dataSource).execute(statsSql);
        writeFile(label, runExplain(dataSource, explainSql));
    }

    /**
     * DBTUNE-16: plano do CAS em lote do Postgres ({@code UPDATE ... IN
     * (...) ... RETURNING}) — capturado com os mesmos 20 ids que a claim
     * query selecionaria, sobre o mesmo seed e estatísticas do plano do
     * SELECT. {@code EXPLAIN ANALYZE} de UPDATE executa o UPDATE de
     * verdade — schema próprio desta captura, nada compartilhado. O SQL
     * com literais é derivado da constante do dialeto, nunca copiado
     * (mesma regra do {@link #ANSI_LITERAL_SQL}).
     */
    private void capturePostgresBatchCasExplain() throws SQLException, IOException {
        DataSource dataSource = PostgresTestSupport.freshSchema();
        seedBacklog(dataSource);
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        jdbcTemplate.execute("ANALYZE mohs_executions, mohs_job_definitions");
        List<String> ids = jdbcTemplate.queryForList("""
                SELECT id FROM mohs_executions
                WHERE state IN ('ENQUEUED', 'RETRY_SCHEDULED') AND scheduled_at <= TIMESTAMP '2026-08-14 12:00:00'
                ORDER BY priority ASC, scheduled_at ASC
                LIMIT 20
                """, String.class);
        String literalSql = PostgresJdbcDialect.BATCH_TRANSITION_TO_RUNNING
                .replace(":leaseExpiresAt", "TIMESTAMP '2026-08-14 12:00:30'")
                .replace(":nodeId", "'explain-node'")
                .replace(":now", "TIMESTAMP '2026-08-14 12:00:00'")
                .replace(":ids", ids.stream().map(id -> "'" + id + "'").collect(Collectors.joining(", ")));
        writeFile("postgresql-batch-cas", runExplain(dataSource, "EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)\n" + literalSql));
    }

    /** {@code SET SHOWPLAN_ALL} precisa ser o único statement do batch (T-SQL) — duas chamadas separadas na mesma conexão. Estatísticas ANTES do showplan (sob showplan nada executa de verdade). */
    private void captureShowplanSqlServer() throws SQLException, IOException {
        DataSource dataSource = SqlServerTestSupport.freshSchema();
        seedBacklog(dataSource);
        JdbcTemplate statistics = new JdbcTemplate(dataSource);
        statistics.execute("UPDATE STATISTICS mohs_executions");
        statistics.execute("UPDATE STATISTICS mohs_job_definitions");
        List<String> lines = new ArrayList<>();
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute(SQLSERVER_SHOWPLAN_ON);
            try (ResultSet rs = statement.executeQuery(SQLSERVER_SELECT_SQL)) {
                appendRows(rs, lines);
            }
        }
        writeFile("sqlserver", lines);
    }

    /**
     * Achado da primeira rodada de {@code BASELINE.md}: MySQL tem
     * latência por chamada parecida com Postgres/SQL Server, mas
     * throughput ~5x pior sob concorrência — hipótese de contenção de
     * row lock (SKIP LOCKED sob REPEATABLE READ). {@code
     * Innodb_row_lock_*} são contadores globais cumulativos desde o
     * start do servidor, sempre disponíveis (sem precisar habilitar
     * monitor) — delta antes/depois do mesmo cenário de 8 nós concorrentes
     * do throughput isola o tempo gasto esperando lock de linha nesta
     * rodada especificamente.
     */
    private void investigateMySqlRowLockContention() throws Exception {
        DataSource raw = MySqlTestSupport.freshSchema();
        seedBacklog(raw);
        Map<String, Long> before = rowLockStatus(raw);

        HikariConfig config = new HikariConfig();
        config.setDataSource(raw);
        config.setMaximumPoolSize(CONCURRENT_NODES * 2);
        config.setConnectionTimeout(Duration.ofSeconds(2).toMillis());
        config.setPoolName("mysql-lock-investigation");
        long elapsedNanos;
        int totalClaimed;
        try (HikariDataSource pool = new HikariDataSource(config)) {
            MutableClock clock = new MutableClock(NOW, ZoneId.of("UTC"));
            JdbcJobStore jobStore = new JdbcJobStore(pool, clock);
            JdbcExecutionStore executionStore = new JdbcExecutionStore(pool, clock, JsonMapper.builder().build(), new MySqlJdbcDialect());
            AtomicInteger claimedTotal = new AtomicInteger();
            ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
            List<Future<?>> nodes = new ArrayList<>();
            long start = System.nanoTime();
            for (int i = 0; i < CONCURRENT_NODES; i++) {
                JdbcClaimer nodeClaimer = new JdbcClaimer(pool, new MySqlJdbcDialect(), clock, executionStore, jobStore, LEASE_TTL, new ExecutionWindowRegistry(List.of()), new JdbcRateLimitStore(pool, clock));
                String nodeId = "lock-investigation-node-" + i;
                nodes.add(executor.submit(() -> {
                    List<Execution> claimed;
                    do {
                        claimed = nodeClaimer.claim(nodeId, BATCH_SIZE);
                        claimedTotal.addAndGet(claimed.size());
                    } while (!claimed.isEmpty());
                }));
            }
            for (Future<?> node : nodes) {
                node.get(60, TimeUnit.SECONDS);
            }
            elapsedNanos = System.nanoTime() - start;
            totalClaimed = claimedTotal.get();
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }

        Map<String, Long> after = rowLockStatus(raw);
        List<String> report = new ArrayList<>();
        report.add("Cenario: " + CONCURRENT_NODES + " nos concorrentes drenando " + totalClaimed
                + " linhas (batchSize=" + BATCH_SIZE + ") em " + (elapsedNanos / 1_000_000) + " ms.");
        report.add("");
        report.add("Innodb_row_lock_* (contadores globais cumulativos do servidor) — delta desta rodada:");
        for (String key : List.of("Innodb_row_lock_waits", "Innodb_row_lock_time", "Innodb_row_lock_time_avg", "Innodb_row_lock_time_max")) {
            long delta = after.getOrDefault(key, 0L) - before.getOrDefault(key, 0L);
            report.add(String.format("  %-28s antes=%-10d depois=%-10d delta=%d", key, before.getOrDefault(key, 0L), after.getOrDefault(key, 0L), delta));
        }
        writeFile("mysql-lock-investigation", report);
    }

    private static Map<String, Long> rowLockStatus(DataSource dataSource) throws SQLException {
        Map<String, Long> values = new HashMap<>();
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery("SHOW STATUS LIKE 'Innodb_row_lock%'")) {
            while (rs.next()) {
                values.put(rs.getString(1), Long.parseLong(rs.getString(2)));
            }
        }
        return values;
    }

    private static List<String> runExplain(DataSource dataSource, String sql) throws SQLException {
        List<String> lines = new ArrayList<>();
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(sql)) {
            appendRows(rs, lines);
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

    /**
     * Mesma forma de {@code JdbcJobStore.upsert} — todo job com
     * {@code allowConcurrentExecutions = true} (isola o custo da query do
     * mutex de job). Seed misto (~20% {@code RETRY_SCHEDULED},
     * determinístico) — mesmo espelho do {@code ClaimQueryLoadHarness}: o
     * plano deve refletir a condição sob a qual os números foram medidos.
     */
    private static void seedBacklog(DataSource dataSource) {
        MutableClock clock = new MutableClock(NOW, ZoneId.of("UTC"));
        JdbcJobStore jobStore = new JdbcJobStore(dataSource, clock);
        JdbcTemplate rawJdbcTemplate = new JdbcTemplate(dataSource);
        List<Object[]> batchArgs = new ArrayList<>();
        for (int j = 0; j < JOB_COUNT; j++) {
            String jobKey = "explain-job-" + j;
            jobStore.upsert(JobDefinition.of(jobKey, Handler.class, spec -> spec.onDemand()));
            for (int e = 0; e < EXECUTIONS_PER_JOB; e++) {
                String state = e % 5 == 0 ? "RETRY_SCHEDULED" : "ENQUEUED";
                batchArgs.add(new Object[] {
                        UUID.randomUUID().toString(), jobKey, state, JdbcTimestamps.toUtcTimestamp(NOW.minusSeconds(1)),
                        Priority.NORMAL.value(), JdbcTimestamps.toUtcTimestamp(NOW)
                });
            }
        }
        rawJdbcTemplate.batchUpdate("""
                INSERT INTO mohs_executions (
                    id, job_key, state, scheduled_at, actor, priority, payload, payload_type, created_at)
                VALUES (?, ?, ?, ?, 'explain-harness', ?, '{}', 'java.lang.Object', ?)
                """, batchArgs);
    }

    private static DataSource freshH2DataSource() {
        JdbcDataSource h2 = new JdbcDataSource();
        h2.setURL("jdbc:h2:mem:claim-explain-harness-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
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
}
