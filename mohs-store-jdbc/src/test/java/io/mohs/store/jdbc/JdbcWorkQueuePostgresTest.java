package io.mohs.store.jdbc;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;

import javax.sql.DataSource;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import io.mohs.core.execution.ExecutionId;
import io.mohs.core.job.JobKey;
import io.mohs.engine.WorkQueue;
import io.mohs.store.jdbc.dialect.PostgresJdbcDialect;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * O claim do §5.4 na forma que só o Tier 1 tem — statement ÚNICO
 * ({@code WITH picked … DELETE … RETURNING → INSERT}) — mais a travessia
 * {@code TIMESTAMPTZ}/{@code OffsetDateTime} das tabelas do split
 * (PLAN.md, decisão 6). Os cenários de semântica compartilhada moram em
 * {@code JdbcWorkQueueTest} (H2, forma portátil); aqui ficam os que
 * provam a FORMA do Postgres.
 */
class JdbcWorkQueuePostgresTest {

    private static final Instant NOW = Instant.parse("2026-08-22T12:00:00Z");

    private DataSource dataSource;
    private JdbcTemplate rawJdbcTemplate;
    private JdbcWorkQueue queue;

    @BeforeEach
    void setUp() {
        dataSource = PostgresTestSupport.freshSchema();
        rawJdbcTemplate = new JdbcTemplate(dataSource);
        queue = new JdbcWorkQueue(dataSource, new PostgresJdbcDialect(), new JdbcBatchStore(dataSource, Clock.fixed(NOW, ZoneOffset.UTC)));
    }

    private WorkQueue.ReadyEntry entry(String id, String jobKey, int priority, int attempt, Instant visibleAt) {
        return new WorkQueue.ReadyEntry(ExecutionId.of(id), JobKey.of(jobKey), 0, priority, attempt, visibleAt);
    }

    @Test
    void singleStatementClaimMovesQueueEntriesIntoLeases() {
        queue.offer(List.of(
                entry("exec-high", "job-a", 10, 1, NOW.minusSeconds(10)),
                entry("exec-normal", "job-a", 20, 2, NOW.minusSeconds(20)),
                entry("exec-future", "job-a", 10, 1, NOW.plusSeconds(60))));

        List<WorkQueue.ClaimedWork> claimed = queue.claim(0, "node-pg", 3, 10, List.of(), NOW);

        // ORDEM pinada de propósito (review S5.2): o RETURNING de INSERT não
        // garante ordem, então o statement devolve do SELECT ordenado sobre
        // picked — o contrato da porta é o mesmo nos 4 dialetos
        assertThat(claimed).extracting(w -> w.executionId().value())
                .containsExactly("exec-high", "exec-normal");
        assertThat(claimed).filteredOn(w -> w.executionId().value().equals("exec-normal"))
                .singleElement().satisfies(w -> assertThat(w.attemptNumber()).isEqualTo(2));
        assertThat(rawJdbcTemplate.queryForList("SELECT execution_id FROM mohs_ready", String.class))
                .containsExactly("exec-future");
        assertThat(rawJdbcTemplate.queryForList(
                "SELECT execution_id FROM mohs_lease WHERE node_id = 'node-pg' AND epoch = 3", String.class))
                .containsExactlyInAnyOrder("exec-high", "exec-normal");
    }

    @Test
    void singleStatementClaimHonoursTheInadmissibleList() {
        queue.offer(List.of(
                entry("exec-open", "job-open", 20, 1, NOW.minusSeconds(2)),
                entry("exec-closed", "job-closed", 10, 1, NOW.minusSeconds(1))));

        List<WorkQueue.ClaimedWork> claimed = queue.claim(0, "node-pg", 1, 10, List.of(JobKey.of("job-closed")), NOW);

        assertThat(claimed).extracting(w -> w.executionId().value()).containsExactly("exec-open");
        assertThat(rawJdbcTemplate.queryForList("SELECT execution_id FROM mohs_ready", String.class))
                .containsExactly("exec-closed");
    }

    /** A travessia OffsetDateTime UTC (decisão 6 do PLAN.md): o instante atravessa a coluna TIMESTAMPTZ verbatim, independente do fuso da sessão. */
    @Test
    void visibleAtRoundTripsThroughTimestamptzVerbatim() {
        Instant visibleAt = Instant.parse("2026-11-01T05:30:00.123456Z");
        queue.offer(List.of(entry("exec-tz", "job-a", 20, 1, visibleAt)));

        Instant readBack = rawJdbcTemplate.queryForObject(
                "SELECT visible_at FROM mohs_ready WHERE execution_id = 'exec-tz'", java.time.OffsetDateTime.class)
                .toInstant();

        assertThat(readBack).isEqualTo(visibleAt);
    }

    /**
     * O coração do claim multi-nó (JCIP cap. 12 — interleaving controlado,
     * não corrida): outra transação segura o lock de uma entrada; o claim
     * PULA a linha lockada sem bloquear — é o {@code SKIP LOCKED} da CTE
     * fazendo o trabalho que o E2 mediu por carga.
     */
    @Test
    void claimSkipsRowsLockedByAConcurrentClaimant() throws Exception {
        queue.offer(List.of(
                entry("exec-locked", "job-a", 10, 1, NOW.minusSeconds(2)),
                entry("exec-free", "job-a", 20, 1, NOW.minusSeconds(1))));
        try (java.sql.Connection other = dataSource.getConnection()) {
            other.setAutoCommit(false);
            try (java.sql.Statement st = other.createStatement()) {
                st.execute("SELECT execution_id FROM mohs_ready WHERE execution_id = 'exec-locked' FOR UPDATE");

                assertThat(queue.claim(0, "node-b", 1, 10, List.of(), NOW))
                        .extracting(w -> w.executionId().value())
                        .containsExactly("exec-free");
            } finally {
                other.rollback();
            }
        }
    }

    @Test
    void requeueFenceHoldsOnPostgresToo() {
        queue.offer(List.of(entry("exec-1", "job-a", 20, 1, NOW.minusSeconds(1))));
        queue.claim(0, "node-pg", 1, 10, List.of(), NOW);

        assertThat(queue.requeue(List.of(new WorkQueue.Requeue(
                ExecutionId.of("exec-1"), "node-pg", 99, entry("exec-1", "job-a", 20, 2, NOW))))).isZero();
        assertThat(queue.requeue(List.of(new WorkQueue.Requeue(
                ExecutionId.of("exec-1"), "node-pg", 1, entry("exec-1", "job-a", 20, 2, NOW))))).isEqualTo(1);
    }
}
