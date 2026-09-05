/*
 * Copyright 2026 The Mohs Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
import io.mohs.store.jdbc.delegate.PostgresJdbcDelegate;

/**
 * The cost of {@code GET /overview} once the database is no longer small. The endpoint is the
 * dashboard's polling anchor and its contract says "cheap by construction"; this scenario is what
 * settles the bill.
 *
 * <p>The two reads the endpoint performs have OPPOSITE cost profiles, and exposing that difference
 * is why the scenario exists:
 * <ul>
 *   <li>{@code countTerminalOutcomesSince} scans {@code mohs_attempt} through the
 *       {@code (finished_at, outcome)} index — it should cost the WINDOW (60s, the default for
 *       {@code ?window=}), not the archive. If that holds, history may grow freely;</li>
 *   <li>{@code countActiveByState} runs {@code COUNT(*)} with NO {@code WHERE} over
 *       {@code mohs_ready} — it costs the whole BACKLOG. This is where the endpoint hurts, and it
 *       is not history that hurts it: it is the queue.</li>
 * </ul>
 *
 * <p>Declared limitation: the plan targets 10^9 history rows, which does not fit on this bench
 * (roughly 100 GB). It seeds what the machine allows and reports the PLAN alongside the number —
 * an index scan touching only the window is evidence that cost does not track the archive; the
 * absolute figure is not.
 *
 * <p>Historical note: {@code mohs_attempt} used to be partitioned by week on Postgres and is now
 * an ordinary table, as in the other dialects. This bench never exercised pruning — the 2M history
 * rows all landed in the DEFAULT partition — and it was precisely that number, 1.6 ms with the
 * {@code (finished_at, outcome)} index resolving it alone, which helped show that partitioning was
 * not buying what it cost.
 *
 * <p>Run by name: {@code ./mvnw -pl mohs-benchmark test -Dtest=OverviewLatencyScenario}.
 */
class OverviewLatencyScenario {

    private static final int HISTORY_ROWS = 2_000_000;
    private static final int BACKLOG_ROWS = 500_000;
    /**
     * Thirty samples do not support a p99 — {@code ceil(0.99 x 30) - 1} returns the LAST element,
     * i.e. the worst of 30. The report calls it "worst" for that reason: promising a p99 here would
     * be selling a tail statistic the sample does not have, and it was exactly one isolated
     * connection outlier that produced the 37.7 ms of this bench's first run.
     */
    private static final int SAMPLES = 30;
    private static final Duration WINDOW = Duration.ofSeconds(60);
    private static final Duration TARGET_P99 = Duration.ofMillis(100);

    @Test
    void theOverviewQueriesStayCheapAsTheHistoryGrows() throws SQLException {
        DataSource dataSource = PostgresTestSupport.freshSchema();
        JdbcTemplate jdbc = new JdbcTemplate(dataSource);
        Clock clock = Clock.systemUTC();
        // ONE connection for the measured reads: PostgresTestSupport's PGSimpleDataSource opens TCP
        // + auth ON EVERY statement (~7ms measured against a query whose EXPLAIN says 0.074ms — 99%
        // of the number would be handshake). What this scenario exists to bill is the QUERY's cost;
        // production pays for the connection once per pool, not once per poll.
        DataSource measured = new SingleConnectionDataSource(dataSource.getConnection(), true);
        JdbcHistoryStore history =
                new JdbcHistoryStore(measured, JsonMapper.builder().build(), new PostgresJdbcDelegate());

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

    /** History seeded in bulk via {@code generate_series}: 2M rows one at a time through the store would take longer than the measurement. */
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
     * What the dashboard pays per hit: both reads happen in the same request, so the ceiling
     * examined is the sum of the two p99s — a deliberately conservative bound that the p99 of the
     * real sum only reaches if both tails coincide.
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

        // The plan is the evidence that survives the bench's size: an index scan touching only the
        // window says cost does not track the archive; the millisecond figure does not
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
