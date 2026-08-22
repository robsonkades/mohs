package io.mohs.store.jdbc;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.sql.DataSource;

import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

import org.jspecify.annotations.Nullable;

import io.mohs.core.execution.ExecutionId;
import io.mohs.core.execution.ExecutionState;
import io.mohs.core.job.JobKey;
import io.mohs.engine.LeaseStore;
import io.mohs.engine.WorkQueue;
import io.mohs.store.jdbc.dialect.H2JdbcDialect;

import static org.assertj.core.api.Assertions.assertThat;

class JdbcLeaseStoreTest {

    private static final Instant NOW = Instant.parse("2026-08-22T12:00:00Z");
    private static final Instant CREATED_AT = NOW.minusSeconds(60);

    private JdbcTemplate rawJdbcTemplate;
    private JdbcLeaseStore store;
    private JdbcWorkQueue queue;

    @BeforeEach
    void setUp() {
        DataSource dataSource = freshH2DataSource();
        rawJdbcTemplate = new JdbcTemplate(dataSource);
        store = new JdbcLeaseStore(dataSource, new H2JdbcDialect());
        queue = new JdbcWorkQueue(dataSource, new H2JdbcDialect());
    }

    private static DataSource freshH2DataSource() {
        JdbcDataSource h2 = new JdbcDataSource();
        h2.setURL("jdbc:h2:mem:lease-store-test-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        h2.setUser("sa");
        h2.setPassword("");
        new ResourceDatabasePopulator(new ClassPathResource("schema-h2.sql")).execute(h2);
        return h2;
    }

    /** Semeia a posse pelo caminho real (fila → claim) e a linha de história que o UPDATE terminal advisory alcança. */
    private void seedLeased(String id, String jobKey, String nodeId, long epoch, int attempt) {
        rawJdbcTemplate.update("""
                INSERT INTO mohs_execution (execution_id, job_key, state, scheduled_at, created_at, actor, payload, payload_type)
                VALUES (?, ?, 'PENDING', ?, ?, 'test', '{}', 'java.lang.Object')
                """, id, jobKey, JdbcTimestamps.toUtcLocalDateTime(CREATED_AT), JdbcTimestamps.toUtcLocalDateTime(CREATED_AT));
        queue.offer(List.of(new WorkQueue.ReadyEntry(ExecutionId.of(id), JobKey.of(jobKey), 0, 20, attempt, NOW.minusSeconds(1))));
        queue.claim(0, nodeId, epoch, 10, List.of(), NOW);
    }

    private LeaseStore.CompletionResult terminal(String id, String jobKey, String nodeId, long epoch, int attempt,
            ExecutionState state, @Nullable String errorType, @Nullable String error) {
        return new LeaseStore.CompletionResult(ExecutionId.of(id), JobKey.of(jobKey), nodeId, epoch, attempt,
                NOW.minusSeconds(2), NOW, state, errorType, error, state, CREATED_AT, null);
    }

    @Test
    void completeDeletesTheLeaseRecordsTheAttemptAndUpdatesTheAdvisoryState() {
        seedLeased("exec-1", "job-a", "node-a", 1, 1);

        Set<ExecutionId> owned = store.complete(List.of(
                terminal("exec-1", "job-a", "node-a", 1, 1, ExecutionState.SUCCEEDED, null, null)));

        assertThat(owned).containsExactly(ExecutionId.of("exec-1"));
        assertThat(rawJdbcTemplate.queryForObject("SELECT COUNT(*) FROM mohs_lease", Integer.class)).isZero();
        assertThat(rawJdbcTemplate.queryForObject(
                "SELECT outcome FROM mohs_attempt WHERE execution_id = 'exec-1' AND number = 1", String.class))
                .isEqualTo("SUCCEEDED");
        assertThat(rawJdbcTemplate.queryForObject(
                "SELECT state FROM mohs_execution WHERE execution_id = 'exec-1'", String.class)).isEqualTo("SUCCEEDED");
    }

    /** O fence do §6.3: um resultado com epoch defasado (zumbi) perde TUDO — nem attempt, nem estado, nem a lease da encarnação nova. */
    @Test
    void completeWithAStaleEpochIsFencedOutEntirely() {
        seedLeased("exec-1", "job-a", "node-a", 2, 1);

        Set<ExecutionId> owned = store.complete(List.of(
                terminal("exec-1", "job-a", "node-a", 1, 1, ExecutionState.SUCCEEDED, null, null)));

        assertThat(owned).isEmpty();
        assertThat(rawJdbcTemplate.queryForObject("SELECT COUNT(*) FROM mohs_lease", Integer.class)).isEqualTo(1);
        assertThat(rawJdbcTemplate.queryForObject("SELECT COUNT(*) FROM mohs_attempt", Integer.class)).isZero();
        assertThat(rawJdbcTemplate.queryForObject(
                "SELECT state FROM mohs_execution WHERE execution_id = 'exec-1'", String.class)).isEqualTo("PENDING");
    }

    /** Um lote misto: o zumbi é descartado linha a linha, nunca contamina os vizinhos (mesma disciplina do completeAll da ADR-0047). */
    @Test
    void completePartitionsFenceWinnersFromLosersWithinOneBatch() {
        seedLeased("exec-win", "job-a", "node-a", 1, 1);
        seedLeased("exec-lose", "job-a", "node-b", 5, 1);

        Set<ExecutionId> owned = store.complete(List.of(
                terminal("exec-win", "job-a", "node-a", 1, 1, ExecutionState.SUCCEEDED, null, null),
                terminal("exec-lose", "job-a", "node-b", 4, 1, ExecutionState.SUCCEEDED, null, null)));

        assertThat(owned).containsExactly(ExecutionId.of("exec-win"));
        assertThat(rawJdbcTemplate.queryForList("SELECT execution_id FROM mohs_lease", String.class))
                .containsExactly("exec-lose");
    }

    /** Retry não-terminal: o renascimento na fila é NA MESMA transação — sem janela de execução órfã (ver Javadoc de CompletionResult.retry). */
    @Test
    void completeWithARetryRebirthsTheReadyEntryAtomically() {
        seedLeased("exec-1", "job-a", "node-a", 1, 1);

        Set<ExecutionId> owned = store.complete(List.of(new LeaseStore.CompletionResult(
                ExecutionId.of("exec-1"), JobKey.of("job-a"), "node-a", 1, 1,
                NOW.minusSeconds(2), NOW, ExecutionState.FAILED, "java.lang.IllegalStateException", "boom",
                null, CREATED_AT,
                new WorkQueue.ReadyEntry(ExecutionId.of("exec-1"), JobKey.of("job-a"), 0, 20, 2, NOW.plusSeconds(30)))));

        assertThat(owned).containsExactly(ExecutionId.of("exec-1"));
        assertThat(rawJdbcTemplate.queryForObject("SELECT COUNT(*) FROM mohs_lease", Integer.class)).isZero();
        assertThat(rawJdbcTemplate.queryForObject(
                "SELECT error_type FROM mohs_attempt WHERE execution_id = 'exec-1'", String.class))
                .isEqualTo("java.lang.IllegalStateException");
        // a história continua PENDING (não-terminal) e a fila tem a entrada do attempt 2 com o backoff
        assertThat(rawJdbcTemplate.queryForObject(
                "SELECT state FROM mohs_execution WHERE execution_id = 'exec-1'", String.class)).isEqualTo("PENDING");
        assertThat(rawJdbcTemplate.queryForObject(
                "SELECT attempt FROM mohs_ready WHERE execution_id = 'exec-1'", Integer.class)).isEqualTo(2);
    }

    @Test
    void countByJobCountsOnlyTheAskedJobs() {
        seedLeased("exec-1", "job-a", "node-a", 1, 1);
        seedLeased("exec-2", "job-a", "node-a", 1, 1);
        seedLeased("exec-3", "job-b", "node-a", 1, 1);

        Map<JobKey, Integer> counts = store.countByJob(List.of(JobKey.of("job-a")));

        assertThat(counts).containsExactly(Map.entry(JobKey.of("job-a"), 2));
    }

    @Test
    void findByNodesReturnsTheNodesLeases() {
        seedLeased("exec-1", "job-a", "node-a", 1, 1);
        seedLeased("exec-2", "job-a", "node-b", 3, 2);

        List<LeaseStore.Lease> leases = store.findByNodes(List.of("node-b"));

        assertThat(leases).singleElement().satisfies(lease -> {
            assertThat(lease.executionId().value()).isEqualTo("exec-2");
            assertThat(lease.epoch()).isEqualTo(3);
            assertThat(lease.attemptNumber()).isEqualTo(2);
            assertThat(lease.claimedAt()).isEqualTo(NOW);
        });
    }

    /** Cancel cooperativo (ADR-0034) agora mora na posse: flag na lease; execução sem lease não está rodando — false. */
    @Test
    void requestCancellationFlagsOnlyALiveLease() {
        seedLeased("exec-1", "job-a", "node-a", 1, 1);

        assertThat(store.requestCancellation(ExecutionId.of("exec-1"))).isTrue();
        assertThat(store.requestCancellation(ExecutionId.of("ghost"))).isFalse();
        assertThat(store.findCancelRequested(List.of(ExecutionId.of("exec-1"), ExecutionId.of("ghost"))))
                .containsExactly(ExecutionId.of("exec-1"));
    }

    @Test
    void completeOfNothingIsANoOp() {
        assertThat(store.complete(List.of())).isEmpty();
    }
}
