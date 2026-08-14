package io.mohs.jdbc;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.IntStream;

import javax.sql.DataSource;

import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

import tools.jackson.databind.json.JsonMapper;

import io.mohs.core.definition.JobDefinition;
import io.mohs.core.execution.Attempt;
import io.mohs.core.execution.Execution;
import io.mohs.core.execution.ExecutionId;
import io.mohs.core.execution.ExecutionState;
import io.mohs.core.job.JobKey;
import io.mohs.test.MutableClock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JdbcExecutionStoreTest {

    record WelcomeEmail(String user, int age) {
    }

    record Handler() {
    }

    private DataSource dataSource;
    private MutableClock clock;
    private JdbcExecutionStore store;

    @BeforeEach
    void setUp() {
        dataSource = freshH2DataSource();
        clock = new MutableClock(Instant.parse("2026-08-13T00:00:00Z"), ZoneId.of("UTC"));
        store = new JdbcExecutionStore(dataSource, clock, JsonMapper.builder().build());
        seedJobDefinition("welcome-email");
    }

    private static DataSource freshH2DataSource() {
        JdbcDataSource h2 = new JdbcDataSource();
        h2.setURL("jdbc:h2:mem:execution-store-test-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        h2.setUser("sa");
        h2.setPassword("");
        new ResourceDatabasePopulator(new ClassPathResource("schema-h2.sql")).execute(h2);
        return h2;
    }

    /** mohs_executions.job_key tem FK pra mohs_job_definitions — precisa existir antes de inserir uma execução. */
    private void seedJobDefinition(String jobKey) {
        new JdbcJobStore(dataSource, clock).upsert(
                JobDefinition.of(jobKey, Handler.class, spec -> spec.onDemand().runner("io")));
    }

    private static Execution execution(String id, String jobKey) {
        return new Execution(
                ExecutionId.of(id), JobKey.of(jobKey), ExecutionState.ENQUEUED,
                Instant.parse("2026-08-13T00:00:00Z"), null, List.of(), "application");
    }

    @Test
    void insertPersistsAndRoundTripsAnExecution() {
        Execution execution = execution("019abc-1", "welcome-email");

        store.insert(execution, new WelcomeEmail("ana", 31));
        Optional<Execution> found = store.find(ExecutionId.of("019abc-1"));

        assertThat(found).contains(execution);
    }

    @Test
    void insertPersistsThePayloadAsJson() {
        store.insert(execution("019abc-2", "welcome-email"), new WelcomeEmail("ana", 31));

        JdbcTemplate rawJdbcTemplate = new JdbcTemplate(dataSource);
        String payload = rawJdbcTemplate.queryForObject(
                "SELECT payload FROM mohs_executions WHERE id = ?", String.class, "019abc-2");
        String payloadType = rawJdbcTemplate.queryForObject(
                "SELECT payload_type FROM mohs_executions WHERE id = ?", String.class, "019abc-2");

        assertThat(payload).contains("\"user\":\"ana\"", "\"age\":31");
        assertThat(payloadType).isEqualTo(WelcomeEmail.class.getName());
    }

    /**
     * DBTUNE-1: prova o contrato "toda coluna temporal guarda UTC" olhando
     * o valor cru gravado (não o round-trip via {@link #store}, que fecha
     * consigo mesmo em qualquer fuso — só provaria que escrita e leitura
     * são simétricas, não que o valor é UTC de verdade). Compara o
     * wall-clock bruto da coluna contra o wall-clock UTC do instante,
     * calculado à parte — passa em qualquer fuso default de JVM.
     */
    @Test
    void scheduledAtIsStoredAsUtcWallClockRegardlessOfJvmDefaultTimeZone() {
        Instant scheduledAt = Instant.parse("2026-08-13T00:00:00Z");
        store.insert(execution("019abc-3", "welcome-email"), new WelcomeEmail("ana", 31));

        Timestamp raw = new JdbcTemplate(dataSource).queryForObject(
                "SELECT scheduled_at FROM mohs_executions WHERE id = ?", Timestamp.class, "019abc-3");

        assertThat(raw.toLocalDateTime()).isEqualTo(LocalDateTime.ofInstant(scheduledAt, ZoneOffset.UTC));
    }

    @Test
    void insertRejectsAnExecutionWithNonEmptyAttempts() {
        Execution withAttempt = new Execution(
                ExecutionId.of("019abc-3"), JobKey.of("welcome-email"), ExecutionState.RUNNING,
                Instant.parse("2026-08-13T00:00:00Z"), Instant.parse("2026-08-13T00:00:01Z"),
                List.of(new Attempt(1, Instant.parse("2026-08-13T00:00:01Z"), null, ExecutionState.RUNNING, null)),
                "application");

        assertThatThrownBy(() -> store.insert(withAttempt, new WelcomeEmail("ana", 31)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void findReturnsEmptyForUnknownId() {
        assertThat(store.find(ExecutionId.of("ghost"))).isEmpty();
    }

    @Test
    void findReconstructsAttemptsFromTheAttemptsTable() {
        store.insert(execution("019abc-4", "welcome-email"), new WelcomeEmail("ana", 31));
        // simula um attempt já ocorrido: insere direto, sem passar pela
        // store (que ainda não escreve em mohs_attempts nesta etapa —
        // isso é claim/dispatch, etapa 3).
        Timestamp startedAt = JdbcTimestamps.toUtcTimestamp(Instant.parse("2026-08-13T00:00:01Z"));
        new JdbcTemplate(dataSource).update("""
                INSERT INTO mohs_attempts (execution_id, number, started_at, outcome)
                VALUES (?, 1, ?, 'RUNNING')
                """, "019abc-4", startedAt);

        Execution found = store.find(ExecutionId.of("019abc-4")).orElseThrow();

        assertThat(found.attempts()).containsExactly(
                new Attempt(1, Instant.parse("2026-08-13T00:00:01Z"), null, ExecutionState.RUNNING, null));
    }

    @Test
    void findByJobKeyReturnsOnlyMatchingExecutions() {
        seedJobDefinition("other-job");
        store.insert(execution("019abc-5", "welcome-email"), new WelcomeEmail("a", 1));
        store.insert(execution("019abc-6", "other-job"), new WelcomeEmail("b", 2));

        try (var found = store.findByJobKey(JobKey.of("welcome-email"))) {
            assertThat(found.map(Execution::id)).containsExactly(ExecutionId.of("019abc-5"));
        }
    }

    @Test
    void findAllReturnsEveryExecution() {
        store.insert(execution("019abc-7", "welcome-email"), new WelcomeEmail("a", 1));
        store.insert(execution("019abc-8", "welcome-email"), new WelcomeEmail("b", 2));

        try (var found = store.findAll()) {
            assertThat(found.map(Execution::id)).containsExactlyInAnyOrder(
                    ExecutionId.of("019abc-7"), ExecutionId.of("019abc-8"));
        }
    }

    @Test
    void findByIdsReturnsOnlyTheRequestedExecutionsInASingleQuery() {
        store.insert(execution("019abc-9", "welcome-email"), new WelcomeEmail("a", 1));
        store.insert(execution("019abc-10", "welcome-email"), new WelcomeEmail("b", 2));
        store.insert(execution("019abc-11", "welcome-email"), new WelcomeEmail("c", 3));

        List<Execution> found = store.findByIds(List.of(ExecutionId.of("019abc-9"), ExecutionId.of("019abc-11")));

        assertThat(found).extracting(Execution::id).containsExactlyInAnyOrder(
                ExecutionId.of("019abc-9"), ExecutionId.of("019abc-11"));
    }

    @Test
    void findByIdsReturnsEmptyForAnEmptyList() {
        assertThat(store.findByIds(List.of())).isEmpty();
    }

    /** DB-11: SQL Server rejeita mais de 2100 parâmetros num `IN (:ids)` — findByIds particiona em lotes. */
    @Test
    void findByIdsHandlesMoreIdsThanFitInASingleInClause() {
        List<ExecutionId> ids = IntStream.range(0, JdbcExecutionStore.MAX_IDS_PER_QUERY + 5)
                .mapToObj(i -> ExecutionId.of("boundary-" + i))
                .toList();
        ids.forEach(id -> store.insert(execution(id.value(), "welcome-email"), new WelcomeEmail("a", 1)));

        List<Execution> found = store.findByIds(ids);

        assertThat(found).extracting(Execution::id).containsExactlyInAnyOrderElementsOf(ids);
    }
}
