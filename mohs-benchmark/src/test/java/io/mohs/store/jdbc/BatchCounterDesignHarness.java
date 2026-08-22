package io.mohs.store.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Evidência para a decisão de contagem de lote: contador MANTIDO numa linha
 * de {@code mohs_batches} (o que o esqueleto atual faz) versus contador
 * DERIVADO de {@code mohs_executions.batch_id}. É a mesma escolha que a
 * ADR-0009 fez para concorrência e que a ADR-0042 NÃO pôde fazer para rate
 * limit — lá derivar era varrer histórico; aqui é um range por índice
 * limitado aos membros de um lote.
 *
 * <p>Quatro eixos, porque decidem coisas diferentes:
 * <ul>
 * <li><b>A) Leitura</b> — {@code GET /batches/{id}}. Mantido é seek na PK;
 * derivado é agregação sobre os membros. O custo do derivado cresce com o
 * tamanho do lote, então a pergunta não é "qual é mais rápido" (é o
 * mantido, obviamente) e sim "a partir de que tamanho de lote o derivado
 * deixa de ser barato o bastante para um dashboard".
 * <li><b>B) Sonda de conclusão</b> — o derivado não avisa ninguém quando
 * chega a zero, alguém tem que perguntar. A pergunta certa não é a
 * agregação: é {@code EXISTS} de membro vivo, que para no primeiro que
 * achar.
 * <li><b>C) Índice</b> — quanto do custo do derivado é do desenho e quanto
 * é do índice de hoje ser {@code (batch_id)} só. Sem este eixo o derivado
 * seria condenado por uma limitação que uma migração resolve.
 * <li><b>D) Escrita</b> — o preço do mantido. Todo membro que termina soma
 * na MESMA linha, no ponto de operação de 4k/s. É a linha quente que a
 * ADR-0042 mediu para o balde, com uma diferença: lá era uma linha para
 * todo o tráfego, aqui é uma por lote.
 * </ul>
 *
 * <p>Fora da suíte unitária de propósito (CLAUDE.md: benchmarks vivem
 * apart) — o nome não casa o include default do Surefire. Sob demanda:
 * {@code mvn test -Dtest=BatchCounterDesignHarness}. Precisa do Docker.
 */
class BatchCounterDesignHarness {

    /** Ruído: histórico terminal fora de qualquer lote, para o índice ter que trabalhar. */
    private static final int BACKGROUND_ROWS = 500_000;

    private static final int[] BATCH_SIZES = {100, 1_000, 10_000, 100_000};

    /** O maior lote é o pior caso de todo eixo: mais membros para a agregação somar e para a sonda percorrer. */
    private static final int LARGEST_BATCH = BATCH_SIZES[BATCH_SIZES.length - 1];

    private static final int[] NODE_COUNTS = {1, 2, 4, 8};

    private static final int SAMPLES = 50;
    private static final int WARMUP = 10;

    private static final Duration WRITE_RUN = Duration.ofSeconds(5);

    /** Estados não terminais — o predicado da sonda de conclusão. */
    private static final String LIVE_STATES = "'ENQUEUED', 'RUNNING', 'RETRY_WAITING'";

    private static final String DERIVED_COUNT =
            "SELECT state, count(*) FROM mohs_executions WHERE batch_id = ? GROUP BY state";

    private static final String KEPT_COUNT =
            "SELECT total, succeeded, failed FROM mohs_batches WHERE id = ?";

    private static final String COMPLETION_PROBE =
            "SELECT 1 FROM mohs_executions WHERE batch_id = ? AND state IN (" + LIVE_STATES + ") LIMIT 1";

    /** O que o {@code JdbcBatchStore} faz hoje: soma sem olhar, uma ida ao banco. */
    private static final String BLIND_INCREMENT =
            "UPDATE mohs_batches SET succeeded = succeeded + 1 WHERE id = ?";

    private static final String READ_COUNTER = "SELECT succeeded FROM mohs_batches WHERE id = ?";

    /** A guarda sobre o valor lido é o que faz o banco eleger QUEM fechou o lote. */
    private static final String CAS_INCREMENT =
            "UPDATE mohs_batches SET succeeded = ? WHERE id = ? AND succeeded = ?";

    private static final String COMPOSITE_INDEX = "idx_bench_batch_state";

    private DataSource dataSource;

    @Test
    void run() throws Exception {
        dataSource = PostgresTestSupport.freshSchema();
        seed();

        System.out.printf("%n=== Seed: %d linhas de ruído + lotes de %s ===%n",
                BACKGROUND_ROWS, Arrays.toString(BATCH_SIZES));

        readLatency();
        completionProbe();
        indexComparison();
        writeContention();
    }

    // ---------------------------------------------------------------- seed

    private void seed() {
        JdbcTemplate jdbc = new JdbcTemplate(dataSource);
        seedJobDefinition(jdbc);
        seedBackgroundNoise(jdbc);
        for (int size : BATCH_SIZES) {
            seedOpenBatch(jdbc, size);
        }
        seedClosedBatch(jdbc);
        refreshStatistics(jdbc);
    }

    /** Só para as execuções terem um job existente; qual job é irrelevante para o que se mede. */
    private static void seedJobDefinition(JdbcTemplate jdbc) {
        jdbc.execute("""
                INSERT INTO mohs_job_definitions
                    (id, job_key, handler_type, schedule_type, misfire, source, created_at, updated_at)
                VALUES ('j1', 'bench-job', 'Handler', 'ON_DEMAND', 'FIRE_NOW', 'PROGRAMMATIC', now(), now())
                """);
    }

    /** Histórico terminal sem lote: é ele que domina a tabela em produção, e sem ele o índice não tem que trabalhar. */
    private static void seedBackgroundNoise(JdbcTemplate jdbc) {
        jdbc.update("""
                INSERT INTO mohs_executions
                    (id, job_key, state, scheduled_at, actor, priority, payload, payload_type, created_at)
                SELECT 'noise-' || lpad(n::text, 9, '0'), 'bench-job', 'SUCCEEDED', now(), 'bench', 20,
                       '{}', 'java.util.Map', now()
                FROM generate_series(1, ?) n
                """, BACKGROUND_ROWS);
    }

    /**
     * Lote ABERTO: 10% dos membros ainda vivos — o caso em que a sonda de
     * conclusão tem que dizer "ainda não" e a agregação tem que somar.
     */
    private static void seedOpenBatch(JdbcTemplate jdbc, int size) {
        jdbc.update("INSERT INTO mohs_batches (id, total, succeeded, failed, created_at) VALUES (?, ?, 0, 0, now())",
                openBatchId(size), size);
        jdbc.update("""
                INSERT INTO mohs_executions
                    (id, job_key, state, scheduled_at, actor, priority, batch_id, payload, payload_type, created_at)
                SELECT ? || '-' || lpad(n::text, 9, '0'), 'bench-job',
                       CASE WHEN n % 10 = 0 THEN 'RUNNING'
                            WHEN n % 10 = 1 THEN 'FAILED'
                            ELSE 'SUCCEEDED' END,
                       now(), 'bench', 20, ?, '{}', 'java.util.Map', now()
                FROM generate_series(1, ?) n
                """, openBatchId(size), openBatchId(size), size);
    }

    /**
     * Lote FECHADO do maior tamanho: pior caso da sonda, que precisa provar
     * ausência em vez de achar o primeiro vivo e parar.
     */
    private static void seedClosedBatch(JdbcTemplate jdbc) {
        jdbc.update("INSERT INTO mohs_batches (id, total, succeeded, failed, created_at) VALUES (?, ?, ?, 0, now())",
                closedBatchId(), LARGEST_BATCH, LARGEST_BATCH);
        jdbc.update("""
                INSERT INTO mohs_executions
                    (id, job_key, state, scheduled_at, actor, priority, batch_id, payload, payload_type, created_at)
                SELECT ? || '-' || lpad(n::text, 9, '0'), 'bench-job', 'SUCCEEDED',
                       now(), 'bench', 20, ?, '{}', 'java.util.Map', now()
                FROM generate_series(1, ?) n
                """, closedBatchId(), closedBatchId(), LARGEST_BATCH);
    }

    /** Sem estatísticas do volume semeado o planner escolhe pelo palpite da tabela vazia. */
    private static void refreshStatistics(JdbcTemplate jdbc) {
        jdbc.execute("ANALYZE mohs_executions");
        jdbc.execute("ANALYZE mohs_batches");
    }

    private static String openBatchId(int size) {
        return "batch-open-" + size;
    }

    private static String closedBatchId() {
        return "batch-closed";
    }

    // ------------------------------------------------------------- leitura

    /**
     * Absolutos, nunca a razão entre os dois: o mantido é seek de PK numa
     * tabela de 5 linhas em cache, então o tempo dele é essencialmente o round
     * trip até o container. Dividir por isso mede a rede, não o desenho — numa
     * rede real de 0,5ms a mesma razão daria outro número sem que nada do que
     * se compara tivesse mudado.
     */
    private void readLatency() throws SQLException {
        System.out.printf("%n=== A) Leitura de GET /batches/{id} (ms) ===%n");
        System.out.printf("%-12s %10s %10s %10s %10s %10s %10s%n",
                "membros", "der p50", "der p95", "der max", "man p50", "man p95", "man max");
        for (int size : BATCH_SIZES) {
            Latency derived = sample(DERIVED_COUNT, openBatchId(size));
            Latency kept = sample(KEPT_COUNT, openBatchId(size));
            System.out.printf("%-12d %10.3f %10.3f %10.3f %10.3f %10.3f %10.3f%n",
                    size, derived.p50Ms(), derived.p95Ms(), derived.maxMs(),
                    kept.p50Ms(), kept.p95Ms(), kept.maxMs());
        }
        System.out.printf("%n--- plano: agregação derivada, lote de %d ---%n", LARGEST_BATCH);
        explain(DERIVED_COUNT, openBatchId(LARGEST_BATCH));
    }

    /**
     * O lote aberto mede o caso comum, em que a sonda acha um vivo e para; o
     * FECHADO mede o caso que decide, o único em que ela percorre tudo para
     * provar ausência — e é justamente quando ela precisa responder.
     */
    private void completionProbe() throws SQLException {
        System.out.printf("%n=== B) Sonda de conclusão do desenho derivado (ms) ===%n");
        System.out.printf("%-28s %10s %10s%n", "cenário", "p50", "p95");
        for (int size : BATCH_SIZES) {
            Latency probe = sample(COMPLETION_PROBE, openBatchId(size));
            System.out.printf("%-28s %10.3f %10.3f%n", "aberto, " + size + " membros", probe.p50Ms(), probe.p95Ms());
        }
        Latency closed = sample(COMPLETION_PROBE, closedBatchId());
        System.out.printf("%-28s %10.3f %10.3f%n",
                "FECHADO, " + LARGEST_BATCH + " membros", closed.p50Ms(), closed.p95Ms());

        System.out.printf("%n--- plano: sonda no lote FECHADO (pior caso, prova ausência) ---%n");
        explain(COMPLETION_PROBE, closedBatchId());
    }

    /**
     * O índice de hoje é {@code (batch_id)} só. A sonda filtra por
     * {@code batch_id AND state}, então a pergunta é se o composto se paga —
     * num lote fechado de 100k o índice simples devolve 100k tuplas para o
     * filtro descartar uma a uma.
     */
    private void indexComparison() throws SQLException {
        System.out.printf("%n=== C) Índice (batch_id) vs (batch_id, state) ===%n");
        JdbcTemplate jdbc = new JdbcTemplate(dataSource);
        // TRUNCATE do freshSchema não derruba índice: resíduo de uma rodada
        // interrompida sobreviveria e mudaria o plano de outra classe na mesma JVM.
        jdbc.execute("DROP INDEX IF EXISTS " + COMPOSITE_INDEX);
        jdbc.execute("CREATE INDEX " + COMPOSITE_INDEX + " ON mohs_executions (batch_id, state)");
        jdbc.execute("ANALYZE mohs_executions");

        System.out.printf("%-28s %10s %10s%n", "cenário (com composto)", "p50", "p95");
        Latency closed = sample(COMPLETION_PROBE, closedBatchId());
        System.out.printf("%-28s %10.3f %10.3f%n", "sonda, lote FECHADO 100k", closed.p50Ms(), closed.p95Ms());
        Latency derived = sample(DERIVED_COUNT, openBatchId(LARGEST_BATCH));
        System.out.printf("%-28s %10.3f %10.3f%n", "agregação, lote 100k", derived.p50Ms(), derived.p95Ms());

        System.out.printf("%n--- plano: sonda no lote FECHADO, com o composto ---%n");
        explain(COMPLETION_PROBE, closedBatchId());
        System.out.printf("%n--- plano: agregação 100k, com o composto ---%n");
        explain(DERIVED_COUNT, openBatchId(LARGEST_BATCH));

        // O composto é a hipótese em avaliação, não o schema: sai antes do eixo D medir a escrita.
        jdbc.execute("DROP INDEX " + COMPOSITE_INDEX);
        jdbc.execute("ANALYZE mohs_executions");
    }

    // ------------------------------------------------------------- escrita

    /**
     * O preço do contador mantido: N nós somando na MESMA linha, que é o que
     * acontece quando um lote grande drena no ponto de operação. O derivado
     * não aparece aqui porque não escreve nada — a conclusão da execução já
     * ia acontecer de qualquer jeito.
     */
    private void writeContention() throws Exception {
        System.out.printf("%n=== D) Escrita: incremento na linha do lote, %ds por nível ===%n", WRITE_RUN.toSeconds());
        System.out.printf("%-6s %14s %14s%n", "nós", "cego (upd/s)", "CAS (upd/s)");
        for (int nodes : NODE_COUNTS) {
            long blind = updatesPerSecond(nodes, BatchCounterDesignHarness::blindIncrement);
            long cas = updatesPerSecond(nodes, BatchCounterDesignHarness::casIncrement);
            System.out.printf("%-6d %14d %14d%n", nodes, blind, cas);
        }
    }

    /**
     * Uma tentativa de somar 1 no contador do lote; {@code true} quando a
     * linha mudou. A tentativa perdida do CAS devolve {@code false}, e é
     * essa diferença — trabalho feito versus trabalho aproveitado — que a
     * comparação entre os dois incrementos mede.
     */
    @FunctionalInterface
    private interface Increment {

        boolean applyTo(Connection connection) throws SQLException;
    }

    /**
     * Todo nível recomeça do mesmo contador: o CAS decide a partir do valor
     * lido, então herdar o saldo do nível anterior tornaria os níveis
     * incomparáveis.
     */
    private long updatesPerSecond(int nodes, Increment increment) throws Exception {
        new JdbcTemplate(dataSource).update("UPDATE mohs_batches SET succeeded = 0 WHERE id = ?", closedBatchId());

        AtomicLong updates = new AtomicLong();
        CyclicBarrier start = new CyclicBarrier(nodes);
        List<Callable<Void>> workers = new ArrayList<>(nodes);
        for (int node = 0; node < nodes; node++) {
            workers.add(hammerUntilDeadline(increment, start, updates));
        }

        ExecutorService pool = Executors.newFixedThreadPool(nodes);
        try {
            long startedAt = System.nanoTime();
            for (Future<Void> future : pool.invokeAll(workers)) {
                future.get();
            }
            double seconds = (System.nanoTime() - startedAt) / 1_000_000_000.0;
            return Math.round(updates.get() / seconds);
        } finally {
            pool.shutdownNow();
        }
    }

    /** Os nós largam juntos (barrier): quem chegasse primeiro mediria a linha sem disputa, o oposto do cenário. */
    private Callable<Void> hammerUntilDeadline(Increment increment, CyclicBarrier start, AtomicLong updates) {
        return () -> {
            try (Connection connection = dataSource.getConnection()) {
                start.await(1, TimeUnit.MINUTES);
                long deadline = System.nanoTime() + WRITE_RUN.toNanos();
                while (System.nanoTime() < deadline) {
                    if (increment.applyTo(connection)) {
                        updates.incrementAndGet();
                    }
                }
            }
            return null;
        };
    }

    private static boolean blindIncrement(Connection connection) throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement(BLIND_INCREMENT)) {
            ps.setString(1, closedBatchId());
            return ps.executeUpdate() == 1;
        }
    }

    private static boolean casIncrement(Connection connection) throws SQLException {
        int current;
        try (PreparedStatement ps = connection.prepareStatement(READ_COUNTER)) {
            ps.setString(1, closedBatchId());
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                current = rs.getInt(1);
            }
        }
        try (PreparedStatement ps = connection.prepareStatement(CAS_INCREMENT)) {
            ps.setInt(1, current + 1);
            ps.setString(2, closedBatchId());
            ps.setInt(3, current);
            return ps.executeUpdate() == 1;
        }
    }

    // -------------------------------------------------------------- utilitários

    /** O warmup é descartado porque as primeiras chamadas pagam plano e cache frio, que não é o que se compara. */
    private Latency sample(String sql, String batchId) throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            for (int i = 0; i < WARMUP; i++) {
                executeAndDrain(connection, sql, batchId);
            }
            long[] samplesNanos = new long[SAMPLES];
            for (int i = 0; i < SAMPLES; i++) {
                long startedAt = System.nanoTime();
                executeAndDrain(connection, sql, batchId);
                samplesNanos[i] = System.nanoTime() - startedAt;
            }
            return Latency.of(samplesNanos);
        }
    }

    /** Lê o result set inteiro: cronometrar só o {@code executeQuery} deixaria de fora o fetch, que é onde a agregação derivada custa. */
    private static void executeAndDrain(Connection connection, String sql, String batchId) throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, batchId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    rs.getObject(1);
                }
            }
        }
    }

    /**
     * Com bind, não com o id inlinado como literal: {@link #sample} mede via
     * {@code PreparedStatement}, e literal força custom plan — o plano impresso
     * como prova tem que ser o da query cronometrada, não o de uma prima dela.
     */
    private void explain(String sql, String batchId) throws SQLException {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement("EXPLAIN (ANALYZE, BUFFERS) " + sql)) {
            statement.setString(1, batchId);
            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    System.out.println("  " + rs.getString(1));
                }
            }
        }
    }

    /**
     * Com {@link #SAMPLES} amostras não existe p99 — {@code sorted[(int) (50 * 0.99)]}
     * é {@code sorted[49]}, o máximo. O maior quantil honesto aqui é p95, e o
     * máximo aparece com o nome dele.
     */
    private record Latency(double p50Ms, double p95Ms, double maxMs) {

        static Latency of(long[] samplesNanos) {
            long[] sorted = samplesNanos.clone();
            Arrays.sort(sorted);
            return new Latency(toMillis(sorted[sorted.length / 2]),
                    toMillis(sorted[(int) (sorted.length * 0.95)]),
                    toMillis(sorted[sorted.length - 1]));
        }

        private static double toMillis(long nanos) {
            return nanos / 1_000_000.0;
        }
    }
}
