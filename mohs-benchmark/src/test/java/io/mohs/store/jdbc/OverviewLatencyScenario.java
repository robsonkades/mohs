package io.mohs.store.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;

import tools.jackson.databind.json.JsonMapper;

import io.mohs.core.execution.ExecutionState;
import io.mohs.store.jdbc.dialect.PostgresJdbcDialect;

/**
 * S5 do §20.2 — o custo do {@code GET /overview} quando o banco não é
 * mais pequeno. O endpoint é a âncora de polling do dashboard e o
 * contrato dele diz "barato por construção"; este cenário é quem cobra a
 * conta.
 *
 * <p>As duas leituras que o endpoint faz têm perfis de custo OPOSTOS, e é
 * essa diferença que o cenário existe para expor:
 * <ul>
 *   <li>{@code countTerminalOutcomesSince} varre {@code mohs_attempt} pelo
 *       índice {@code (finished_at, outcome)} — deve custar a JANELA (60s,
 *       o default do {@code ?window=}), não o acervo. Se isso valer, a
 *       história pode crescer à vontade;</li>
 *   <li>{@code countActiveByState} faz {@code COUNT(*)} SEM {@code WHERE}
 *       em {@code mohs_ready} — custa o BACKLOG inteiro. É aqui que o
 *       endpoint dói, e não é a história que o machuca: é a fila.</li>
 * </ul>
 *
 * <p>Limite declarado: o alvo do plano é 10⁹ linhas de história, que não
 * cabe nesta bancada (seriam ~100 GB). Semeia-se o que a máquina permite
 * e reporta-se o PLANO junto do número — um Index Scan que toca só a
 * janela é evidência de que o custo não acompanha o acervo; o número
 * absoluto, não.
 *
 * <p>Nota histórica: até a ADR-0058, {@code mohs_attempt} era particionada
 * por semana no Postgres; hoje é tabela normal, como nos outros dialetos.
 * Esta bancada nunca exercitou o pruning — os 2M de história caíam todos na
 * partição DEFAULT —, e foi justamente esse número, 1,6 ms com o índice
 * {@code (finished_at, outcome)} resolvendo sozinho, que ajudou a mostrar
 * que o particionamento não estava comprando o que custava.
 *
 * <p>Roda por nome: {@code ./mvnw -pl mohs-benchmark test
 * -Dtest=OverviewLatencyScenario}.
 */
class OverviewLatencyScenario {

    private static final int HISTORY_ROWS = 2_000_000;
    private static final int BACKLOG_ROWS = 500_000;
    /**
     * Trinta amostras não sustentam um p99 — {@code ceil(0.99 × 30) − 1}
     * devolve o ÚLTIMO elemento, ou seja o pior de 30. O relatório chama de
     * "worst" por isso: prometer p99 aqui seria vender uma estatística de
     * cauda que a amostra não tem, e foi exatamente um outlier isolado de
     * conexão que produziu o 37,7 ms da primeira rodada desta bancada.
     */
    private static final int SAMPLES = 30;
    private static final Duration WINDOW = Duration.ofSeconds(60);
    private static final Duration TARGET_P99 = Duration.ofMillis(100);

    @Test
    void theOverviewQueriesStayCheapAsTheHistoryGrows() throws SQLException {
        DataSource dataSource = PostgresTestSupport.freshSchema();
        JdbcTemplate jdbc = new JdbcTemplate(dataSource);
        Clock clock = Clock.systemUTC();
        // UMA conexão para as leituras medidas: o PGSimpleDataSource do
        // PostgresTestSupport abre TCP + auth A CADA statement (~7ms medidos
        // contra uma query cujo EXPLAIN diz 0,074ms — 99% do número seria
        // handshake). O que este cenário existe pra cobrar é o custo da
        // QUERY; produção paga a conexão uma vez por pool, não por poll.
        DataSource measured = new SingleConnectionDataSource(dataSource.getConnection(), true);
        JdbcHistoryStore history =
                new JdbcHistoryStore(measured, JsonMapper.builder().build(), new PostgresJdbcDialect());

        seedHistory(jdbc);
        seedBacklog(jdbc);
        jdbc.execute("ANALYZE mohs_attempt");
        jdbc.execute("ANALYZE mohs_ready");

        // warmup fora do registro: a primeira chamada paga plano e cache frio
        for (int i = 0; i < 5; i++) {
            history.countActiveByState(clock.instant());
            history.countTerminalOutcomesSince(clock.instant().minus(WINDOW));
        }

        List<Long> activeNanos = new ArrayList<>();
        List<Long> throughputNanos = new ArrayList<>();
        for (int i = 0; i < SAMPLES; i++) {
            long beforeActive = System.nanoTime();
            history.countActiveByState(clock.instant());
            activeNanos.add(System.nanoTime() - beforeActive);
            long beforeThroughput = System.nanoTime();
            history.countTerminalOutcomesSince(clock.instant().minus(WINDOW));
            throughputNanos.add(System.nanoTime() - beforeThroughput);
        }

        long backlogSeen = history.countActiveByState(clock.instant()).get(ExecutionState.ENQUEUED);
        report(jdbc, activeNanos, throughputNanos, backlogSeen);

        assertThat(Duration.ofNanos(combinedP99(activeNanos, throughputNanos)))
                .as("GET /overview p99 must stay under %d ms with %,d history rows and a %,d backlog — "
                        + "it is the dashboard's polling anchor and the contract calls it cheap by construction",
                        TARGET_P99.toMillis(), HISTORY_ROWS, BACKLOG_ROWS)
                .isLessThan(TARGET_P99);
    }

    /** História em lotes por {@code generate_series}: semear 2M linha a linha pelo store levaria mais que a medição. */
    private static void seedHistory(JdbcTemplate jdbc) {
        jdbc.update("""
                INSERT INTO mohs_attempt (execution_id, number, node_id, started_at, finished_at, outcome)
                SELECT 'hist-' || g, 1, 'seed-node',
                       now() - (g || ' seconds')::interval,
                       now() - (g || ' seconds')::interval,
                       CASE WHEN g % 7 = 0 THEN 'FAILED' ELSE 'SUCCEEDED' END
                  FROM generate_series(1, ?) g
                """, HISTORY_ROWS);
    }

    private static void seedBacklog(JdbcTemplate jdbc) {
        jdbc.update("""
                INSERT INTO mohs_ready (execution_id, job_key, shard, priority, attempt, visible_at)
                SELECT 'ready-' || g, 'seed-job', g % 64, 20, 1, now()
                  FROM generate_series(1, ?) g
                """, BACKLOG_ROWS);
    }

    /**
     * O que o dashboard paga por batida: as duas leituras acontecem no
     * mesmo request, então o teto olhado é a soma dos p99 — um limite
     * conservador de propósito, que o p99 da soma real só alcança se as
     * duas caudas coincidirem.
     */
    private static long combinedP99(List<Long> active, List<Long> throughput) {
        return percentile(active, 0.99) + percentile(throughput, 0.99);
    }

    private static long percentile(List<Long> samples, double p) {
        List<Long> sorted = samples.stream().sorted().toList();
        return sorted.get(Math.min(sorted.size() - 1, (int) Math.ceil(p * sorted.size()) - 1));
    }

    private static void report(JdbcTemplate jdbc, List<Long> active, List<Long> throughput, long backlogSeen) {
        System.out.printf("""

                === S5 — GET /overview under history pressure ===
                seeded               : %,d attempt rows · %,d ready rows (backlog seen: %,d)
                countActiveByState   : p50 %.1f ms · worst %.1f ms   <- scales with the BACKLOG
                countTerminalOutcomes: p50 %.1f ms · worst %.1f ms   <- should scale with the WINDOW, not the history
                worst combined       : %.1f ms (target < %d ms)
                """, HISTORY_ROWS, BACKLOG_ROWS, backlogSeen,
                millis(percentile(active, 0.5)), millis(percentile(active, 0.99)),
                millis(percentile(throughput, 0.5)), millis(percentile(throughput, 0.99)),
                millis(combinedP99(active, throughput)), TARGET_P99.toMillis());

        // o plano é a evidência que sobrevive ao tamanho da bancada: um index
        // scan que toca só a janela diz que o custo não acompanha o acervo; o
        // milissegundo, não
        explain(jdbc, "throughput query (the one that must ignore the history)", """
                SELECT outcome, COUNT(*) FROM mohs_attempt
                 WHERE finished_at >= now() - interval '%d seconds' AND outcome IN ('SUCCEEDED','FAILED')
                 GROUP BY outcome
                """.formatted(WINDOW.toSeconds()));
        explain(jdbc, "backlog count (the one that scales with the queue)", "SELECT COUNT(*) FROM mohs_ready");
    }

    private static double millis(long nanos) {
        return nanos / 1_000_000.0;
    }

    private static void explain(JdbcTemplate jdbc, String label, String query) {
        System.out.println("--- plan: " + label + " ---");
        jdbc.queryForList("EXPLAIN (ANALYZE, BUFFERS) " + query, String.class)
                .forEach(line -> System.out.println("  " + line));
    }
}
