package io.mohs.jdbc;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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

import io.mohs.core.definition.JobDefinition;
import io.mohs.core.execution.Execution;
import io.mohs.core.execution.Priority;
import io.mohs.engine.ExecutionWindowRegistry;
import io.mohs.jdbc.dialect.H2JdbcDialect;
import io.mohs.jdbc.dialect.JdbcDialect;
import io.mohs.jdbc.dialect.MySqlJdbcDialect;
import io.mohs.jdbc.dialect.PostgresJdbcDialect;
import io.mohs.jdbc.dialect.SqlServerJdbcDialect;
import io.mohs.test.MutableClock;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Harness de carga da claim query por dialeto (ADR-0022/0023) — mede o
 * custo real fim-a-fim de {@link JdbcClaimer#claim} (bind de parâmetro +
 * rede + plano de execução do banco: {@code SKIP LOCKED} nativo em
 * H2/Postgres/MySQL vs. hint {@code UPDLOCK, ROWLOCK, READPAST} + retry
 * de deadlock em SQL Server), não só o lado Java isolado — é aí que a
 * divergência de dialeto pesa de verdade (motivo pelo qual isto não é
 * JMH: JMH mede bem o bind/mapeamento de linha, mas isola o JVM do custo
 * de rede/plano de execução do banco).
 *
 * <p>Duas fases por dialeto: <b>latência</b> — chamadas sequenciais de
 * {@code claim()} contra um backlog parado, custo puro da query (sem
 * contenção entre nós); <b>throughput</b> — {@link #THROUGHPUT_NODES}
 * nós virtuais disputando o mesmo backlog até esvaziar, custo sob
 * concorrência real (onde o retry de deadlock do SQL Server entra em
 * jogo).
 *
 * <p>Separado da suíte unitária de propósito (CLAUDE.md: "Benchmarks
 * ficam separados da suíte unitária") — o nome não bate com o padrão de
 * inclusão padrão do Surefire ({@code **&#47;*Test.java}), então não
 * roda em {@code mvn test}; só sob demanda: {@code mvn test
 * -Dtest=ClaimQueryLoadHarness}. Resultado documentado em {@code
 * docs/BASELINE.md} — comparação futura sempre contra ele, nunca contra
 * intuição.
 */
class ClaimQueryLoadHarness {

    record Handler() {
    }

    private static final Instant NOW = Instant.parse("2026-08-14T12:00:00Z");
    private static final Duration LEASE_TTL = Duration.ofSeconds(30);
    private static final int BATCH_SIZE = 20;

    private static final int LATENCY_WARMUP = 10;
    private static final int LATENCY_SAMPLES = 50;
    private static final int LATENCY_JOB_COUNT = 150;
    // 1800 linhas cobrem (10 + 50) * 20 = 1200 claims com folga.
    private static final int LATENCY_EXECUTIONS_PER_JOB = 12;

    private static final int THROUGHPUT_NODES = 8;
    private static final int THROUGHPUT_JOB_COUNT = 300;
    private static final int THROUGHPUT_EXECUTIONS_PER_JOB = 10; // 3000 linhas

    @Test
    void run() throws Exception {
        List<DialectResult> results = new ArrayList<>();
        results.add(measure("H2", ClaimQueryLoadHarness::freshH2DataSource, new H2JdbcDialect()));
        results.add(measure("PostgreSQL", PostgresTestSupport::freshSchema, new PostgresJdbcDialect()));
        results.add(measure("MySQL", MySqlTestSupport::freshSchema, new MySqlJdbcDialect()));
        results.add(measure("SQL Server", SqlServerTestSupport::freshSchema, new SqlServerJdbcDialect()));
        printReport(results);
    }

    /**
     * Cada fase abre seu próprio pool sobre um {@code DataSource} fresco
     * ({@code rawDataSourceFactory.get()} chamado uma vez por fase) — nunca
     * compartilha schema entre latência e throughput: a fase de latência
     * propositalmente não drena o backlog inteiro (mede custo por chamada,
     * não exaustão), então linhas sobrando vazariam pra contagem da fase de
     * throughput se dividissem o mesmo banco.
     */
    private DialectResult measure(String label, Supplier<DataSource> rawDataSourceFactory, JdbcDialect dialect) throws Exception {
        LatencyStats latency;
        try (HikariDataSource pool = pooledDataSource(rawDataSourceFactory.get())) {
            latency = measureLatency(pool, dialect);
        }
        double throughputPerSecond;
        try (HikariDataSource pool = pooledDataSource(rawDataSourceFactory.get())) {
            throughputPerSecond = measureThroughput(pool, dialect);
        }
        return new DialectResult(label, latency, throughputPerSecond);
    }

    private HikariDataSource pooledDataSource(DataSource raw) {
        HikariConfig config = new HikariConfig();
        config.setDataSource(raw);
        config.setMaximumPoolSize(THROUGHPUT_NODES * 2);
        config.setConnectionTimeout(Duration.ofSeconds(2).toMillis());
        config.setPoolName("claim-load-harness");
        return new HikariDataSource(config);
    }

    private LatencyStats measureLatency(DataSource dataSource, JdbcDialect dialect) {
        MutableClock clock = new MutableClock(NOW, ZoneId.of("UTC"));
        JdbcTemplate rawJdbcTemplate = new JdbcTemplate(dataSource);
        JdbcJobStore jobStore = new JdbcJobStore(dataSource, clock);
        JdbcExecutionStore executionStore = new JdbcExecutionStore(dataSource, clock, JsonMapper.builder().build());
        JdbcClaimer claimer = new JdbcClaimer(dataSource, dialect, clock, executionStore, jobStore, LEASE_TTL, new ExecutionWindowRegistry(List.of()));
        seedBacklog(rawJdbcTemplate, jobStore, "latency", LATENCY_JOB_COUNT, LATENCY_EXECUTIONS_PER_JOB);

        for (int i = 0; i < LATENCY_WARMUP; i++) {
            claimer.claim("warmup-node", BATCH_SIZE);
        }
        long[] samplesNanos = new long[LATENCY_SAMPLES];
        for (int i = 0; i < LATENCY_SAMPLES; i++) {
            long start = System.nanoTime();
            claimer.claim("latency-node", BATCH_SIZE);
            samplesNanos[i] = System.nanoTime() - start;
        }
        return LatencyStats.of(samplesNanos);
    }

    private double measureThroughput(DataSource dataSource, JdbcDialect dialect) throws Exception {
        MutableClock clock = new MutableClock(NOW, ZoneId.of("UTC"));
        JdbcTemplate rawJdbcTemplate = new JdbcTemplate(dataSource);
        JdbcJobStore jobStore = new JdbcJobStore(dataSource, clock);
        JdbcExecutionStore executionStore = new JdbcExecutionStore(dataSource, clock, JsonMapper.builder().build());
        int totalRows = seedBacklog(rawJdbcTemplate, jobStore, "throughput", THROUGHPUT_JOB_COUNT, THROUGHPUT_EXECUTIONS_PER_JOB);

        AtomicInteger claimedTotal = new AtomicInteger();
        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        List<Future<?>> nodes = new ArrayList<>();
        long start = System.nanoTime();
        for (int i = 0; i < THROUGHPUT_NODES; i++) {
            JdbcClaimer nodeClaimer = new JdbcClaimer(dataSource, dialect, clock, executionStore, jobStore, LEASE_TTL, new ExecutionWindowRegistry(List.of()));
            String nodeId = "throughput-node-" + i;
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
        long elapsedNanos = System.nanoTime() - start;
        executor.shutdown();
        assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        assertThat(claimedTotal.get()).isEqualTo(totalRows);

        double elapsedSeconds = elapsedNanos / 1_000_000_000.0;
        return claimedTotal.get() / elapsedSeconds;
    }

    /** Todo job com {@code allowConcurrentExecutions = true} (default) — isola o custo da query do mutex de job. */
    private int seedBacklog(JdbcTemplate rawJdbcTemplate, JdbcJobStore jobStore, String prefix, int jobCount, int executionsPerJob) {
        List<Object[]> batchArgs = new ArrayList<>();
        for (int j = 0; j < jobCount; j++) {
            String jobKey = prefix + "-job-" + j;
            jobStore.upsert(JobDefinition.of(jobKey, Handler.class, spec -> spec.onDemand()));
            for (int e = 0; e < executionsPerJob; e++) {
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
        return batchArgs.size();
    }

    private static DataSource freshH2DataSource() {
        JdbcDataSource h2 = new JdbcDataSource();
        h2.setURL("jdbc:h2:mem:claim-load-harness-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        h2.setUser("sa");
        h2.setPassword("");
        new ResourceDatabasePopulator(new ClassPathResource("schema-h2.sql")).execute(h2);
        return h2;
    }

    private void printReport(List<DialectResult> results) {
        System.out.println();
        System.out.println("=== ClaimQueryLoadHarness — claim(batchSize=" + BATCH_SIZE + ") ===");
        System.out.printf("%-12s %10s %10s %10s %10s %16s%n", "Dialeto", "min(ms)", "p50(ms)", "p99(ms)", "max(ms)", "throughput(rows/s)");
        for (DialectResult result : results) {
            System.out.printf("%-12s %10.2f %10.2f %10.2f %10.2f %16.1f%n",
                    result.dialect(), result.latency().minMs(), result.latency().p50Ms(),
                    result.latency().p99Ms(), result.latency().maxMs(), result.throughputPerSecond());
        }
        System.out.println();
    }

    record DialectResult(String dialect, LatencyStats latency, double throughputPerSecond) {
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
