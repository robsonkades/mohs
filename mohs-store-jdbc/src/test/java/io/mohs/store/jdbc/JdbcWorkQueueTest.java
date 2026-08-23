package io.mohs.store.jdbc;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.stream.IntStream;
import java.util.Set;
import java.util.UUID;

import javax.sql.DataSource;

import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

import io.mohs.core.definition.JobDefinition;
import io.mohs.core.execution.ExecutionId;
import io.mohs.core.job.JobKey;
import io.mohs.engine.Shards;
import io.mohs.engine.WorkQueue;
import io.mohs.store.jdbc.dialect.H2JdbcDialect;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link JdbcWorkQueue} sobre H2 — a forma portátil do claim (SELECT com
 * lock → DELETE → INSERT da posse). O statement único do Postgres tem o
 * espelho destes cenários em {@code JdbcWorkQueuePostgresTest}.
 */
class JdbcWorkQueueTest {

    record Handler() {
    }

    private static final Instant NOW = Instant.parse("2026-08-22T12:00:00Z");
    private static final String NODE = "node-a";
    private static final long EPOCH = 1;

    private DataSource dataSource;
    private JdbcTemplate rawJdbcTemplate;
    private JdbcWorkQueue queue;

    @BeforeEach
    void setUp() {
        dataSource = freshH2DataSource();
        rawJdbcTemplate = new JdbcTemplate(dataSource);
        queue = new JdbcWorkQueue(dataSource, new H2JdbcDialect(), new JdbcBatchStore(dataSource, Clock.fixed(NOW, ZoneOffset.UTC)));
    }

    /** ADR-0043: membro de lote não rearma — o lote já contou esta falha; re-rodar contaria o desfecho DUAS vezes num lote possivelmente já fechado (mesmo guard do CAS da era anterior). */
    @Test
    void rearmForManualRetryRefusesABatchMember() {
        new JdbcJobStore(dataSource, Clock.fixed(NOW, ZoneOffset.UTC))
                .upsert(JobDefinition.of("job-a", Handler.class, spec -> spec.onDemand()));
        rawJdbcTemplate.update("""
                INSERT INTO mohs_execution (execution_id, job_key, state, scheduled_at, created_at, actor, correlation_id, payload, payload_type)
                VALUES ('exec-member', 'job-a', 'FAILED', ?, ?, 'test', 'batch-1', '{}', 'java.lang.Object')
                """, JdbcTimestamps.toUtcLocalDateTime(NOW), JdbcTimestamps.toUtcLocalDateTime(NOW));

        assertThat(queue.rearmForManualRetry(ExecutionId.of("exec-member"), NOW)).isFalse();
        assertThat(rawJdbcTemplate.queryForObject(
                "SELECT state FROM mohs_execution WHERE execution_id = 'exec-member'", String.class)).isEqualTo("FAILED");
        assertThat(rawJdbcTemplate.queryForObject("SELECT COUNT(*) FROM mohs_ready", Integer.class)).isZero();
    }

    private static DataSource freshH2DataSource() {
        JdbcDataSource h2 = new JdbcDataSource();
        h2.setURL("jdbc:h2:mem:work-queue-test-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        h2.setUser("sa");
        h2.setPassword("");
        new ResourceDatabasePopulator(new ClassPathResource("schema-h2.sql")).execute(h2);
        return h2;
    }

    private WorkQueue.ReadyEntry entry(String id, String jobKey, int priority, int attempt, Instant visibleAt) {
        return new WorkQueue.ReadyEntry(ExecutionId.of(id), JobKey.of(jobKey), 0, priority, attempt, visibleAt);
    }

    /**
     * S6.5: a sonda do gate ocioso ({@code hasVisibleWork}) atravessa o
     * driver com a LISTA de shards do nó — 64 parâmetros num nó único. O
     * binding de coleção é do driver, não do dialeto, então cada um paga o
     * seu teste; o resto do cenário prova o predicado: shard alheio não
     * conta, entrada ainda invisível não conta.
     */
    @Test
    void hasVisibleWorkSeesOnlyVisibleEntriesInTheOwnedShards() {
        List<Integer> owned = IntStream.range(0, Shards.SHARD_COUNT).filter(shard -> shard % 2 == 0).boxed().toList();
        assertThat(queue.hasVisibleWork(owned, NOW)).isFalse();

        queue.offer(List.of(shardedEntry("exec-alheio", 7, NOW.minusSeconds(1))));
        assertThat(queue.hasVisibleWork(owned, NOW)).as("shard de outro nó").isFalse();

        queue.offer(List.of(shardedEntry("exec-futuro", 8, NOW.plusSeconds(60))));
        assertThat(queue.hasVisibleWork(owned, NOW)).as("ainda não visível").isFalse();

        queue.offer(List.of(shardedEntry("exec-devido", 8, NOW.minusSeconds(1))));
        assertThat(queue.hasVisibleWork(owned, NOW)).isTrue();
    }

    private WorkQueue.ReadyEntry shardedEntry(String id, int shard, Instant visibleAt) {
        return new WorkQueue.ReadyEntry(ExecutionId.of(id), JobKey.of("job-a"), shard, 20, 1, visibleAt);
    }

    @Test
    void claimTakesVisibleEntriesInPriorityThenVisibilityOrder() {
        queue.offer(List.of(
                entry("exec-low", "job-a", 30, 1, NOW.minusSeconds(30)),
                entry("exec-high", "job-a", 10, 1, NOW.minusSeconds(10)),
                entry("exec-normal-old", "job-a", 20, 1, NOW.minusSeconds(20)),
                entry("exec-normal-new", "job-a", 20, 1, NOW.minusSeconds(5))));

        List<WorkQueue.ClaimedWork> claimed = queue.claim(0, NODE, EPOCH, 10, List.of(), NOW);

        assertThat(claimed).extracting(w -> w.executionId().value())
                .containsExactly("exec-high", "exec-normal-old", "exec-normal-new", "exec-low");
        // a fila esvaziou (DELETE, não UPDATE de estado — §5.3) e a posse nasceu na mesma transação (§6.2)
        assertThat(rawJdbcTemplate.queryForObject("SELECT COUNT(*) FROM mohs_ready", Integer.class)).isZero();
        assertThat(rawJdbcTemplate.queryForList("SELECT execution_id FROM mohs_lease WHERE node_id = 'node-a' AND epoch = 1", String.class))
                .containsExactlyInAnyOrder("exec-high", "exec-normal-old", "exec-normal-new", "exec-low");
    }

    @Test
    void claimCopiesTheEntryAttemptIntoTheLease() {
        queue.offer(List.of(entry("exec-retry", "job-a", 20, 3, NOW.minusSeconds(1))));

        List<WorkQueue.ClaimedWork> claimed = queue.claim(0, NODE, EPOCH, 10, List.of(), NOW);

        assertThat(claimed).singleElement().satisfies(w -> assertThat(w.attemptNumber()).isEqualTo(3));
        assertThat(rawJdbcTemplate.queryForObject(
                "SELECT attempt_number FROM mohs_lease WHERE execution_id = 'exec-retry'", Integer.class)).isEqualTo(3);
    }

    @Test
    void claimIgnoresEntriesNotYetVisible() {
        queue.offer(List.of(
                entry("exec-due", "job-a", 20, 1, NOW),
                entry("exec-future", "job-a", 20, 1, NOW.plusSeconds(60))));

        List<WorkQueue.ClaimedWork> claimed = queue.claim(0, NODE, EPOCH, 10, List.of(), NOW);

        assertThat(claimed).extracting(w -> w.executionId().value()).containsExactly("exec-due");
        assertThat(rawJdbcTemplate.queryForObject("SELECT COUNT(*) FROM mohs_ready", Integer.class)).isEqualTo(1);
    }

    @Test
    void claimIsBoundedByTheLimit() {
        queue.offer(List.of(
                entry("exec-1", "job-a", 20, 1, NOW.minusSeconds(3)),
                entry("exec-2", "job-a", 20, 1, NOW.minusSeconds(2)),
                entry("exec-3", "job-a", 20, 1, NOW.minusSeconds(1))));

        assertThat(queue.claim(0, NODE, EPOCH, 2, List.of(), NOW)).hasSize(2);
        assertThat(queue.claim(0, NODE, EPOCH, 2, List.of(), NOW)).hasSize(1);
    }

    /** §5.4: um shard por statement — entradas de outro shard são invisíveis à rodada. */
    @Test
    void claimSeesOnlyItsShard() {
        queue.offer(List.of(new WorkQueue.ReadyEntry(ExecutionId.of("exec-other-shard"), JobKey.of("job-a"), 5, 20, 1, NOW.minusSeconds(1))));
        queue.offer(List.of(entry("exec-shard-0", "job-a", 20, 1, NOW.minusSeconds(1))));

        assertThat(queue.claim(0, NODE, EPOCH, 10, List.of(), NOW))
                .extracting(w -> w.executionId().value()).containsExactly("exec-shard-0");
        assertThat(queue.claim(5, NODE, EPOCH, 10, List.of(), NOW))
                .extracting(w -> w.executionId().value()).containsExactly("exec-other-shard");
    }

    /** A lista de inadmissíveis (§5.4) filtra POR JOB na própria query — janela fechada/cap sem folga/handler ausente nunca viram claim + requeue. */
    @Test
    void claimSkipsInadmissibleJobs() {
        queue.offer(List.of(
                entry("exec-open", "job-open", 20, 1, NOW.minusSeconds(2)),
                entry("exec-closed", "job-closed", 10, 1, NOW.minusSeconds(1))));

        List<WorkQueue.ClaimedWork> claimed = queue.claim(0, NODE, EPOCH, 10, List.of(JobKey.of("job-closed")), NOW);

        assertThat(claimed).extracting(w -> w.executionId().value()).containsExactly("exec-open");
        assertThat(rawJdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM mohs_ready WHERE execution_id = 'exec-closed'", Integer.class)).isEqualTo(1);
    }

    @Test
    void claimOfAnEmptyQueueReturnsEmpty() {
        assertThat(queue.claim(0, NODE, EPOCH, 10, List.of(), NOW)).isEmpty();
    }

    /** Requeue (§4.3): a lease cai CERCADA pela encarnação observada e a entrada renasce na fila — o caminho único de recuperação e de perda de admissão. */
    @Test
    void requeueDropsTheLeaseAndRebirthsTheEntryWhenTheFenceHolds() {
        queue.offer(List.of(entry("exec-1", "job-a", 20, 1, NOW.minusSeconds(1))));
        queue.claim(0, NODE, EPOCH, 10, List.of(), NOW);

        int requeued = queue.requeue(List.of(new WorkQueue.Requeue(
                ExecutionId.of("exec-1"), NODE, EPOCH, entry("exec-1", "job-a", 20, 2, NOW.plusSeconds(30)))));

        assertThat(requeued).isEqualTo(1);
        assertThat(rawJdbcTemplate.queryForObject("SELECT COUNT(*) FROM mohs_lease", Integer.class)).isZero();
        assertThat(rawJdbcTemplate.queryForObject(
                "SELECT attempt FROM mohs_ready WHERE execution_id = 'exec-1'", Integer.class)).isEqualTo(2);
    }

    /** O fence do §6.3 em ação: um requeue com epoch defasado (reaper zumbi) perde e NÃO duplica a entrada na fila. */
    @Test
    void requeueWithAStaleFenceLosesAndDoesNotDuplicate() {
        queue.offer(List.of(entry("exec-1", "job-a", 20, 1, NOW.minusSeconds(1))));
        queue.claim(0, NODE, EPOCH, 10, List.of(), NOW);

        int requeued = queue.requeue(List.of(new WorkQueue.Requeue(
                ExecutionId.of("exec-1"), NODE, EPOCH - 1, entry("exec-1", "job-a", 20, 2, NOW))));

        assertThat(requeued).isZero();
        assertThat(rawJdbcTemplate.queryForObject("SELECT COUNT(*) FROM mohs_lease", Integer.class)).isEqualTo(1);
        assertThat(rawJdbcTemplate.queryForObject("SELECT COUNT(*) FROM mohs_ready", Integer.class)).isZero();
    }

    @Test
    void claimAfterRequeueSeesTheRebornEntry() {
        queue.offer(List.of(entry("exec-1", "job-a", 20, 1, NOW.minusSeconds(1))));
        queue.claim(0, NODE, EPOCH, 10, List.of(), NOW);
        queue.requeue(List.of(new WorkQueue.Requeue(
                ExecutionId.of("exec-1"), NODE, EPOCH, entry("exec-1", "job-a", 20, 2, NOW))));

        List<WorkQueue.ClaimedWork> reclaimed = queue.claim(0, "node-b", 7, 10, List.of(), NOW);

        assertThat(reclaimed).singleElement().satisfies(w -> {
            assertThat(w.executionId().value()).isEqualTo("exec-1");
            assertThat(w.attemptNumber()).isEqualTo(2);
        });
        assertThat(Set.copyOf(rawJdbcTemplate.queryForList(
                "SELECT node_id FROM mohs_lease WHERE execution_id = 'exec-1'", String.class))).containsExactly("node-b");
    }
}
