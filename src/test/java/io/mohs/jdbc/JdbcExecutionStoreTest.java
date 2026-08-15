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
import io.mohs.engine.ExecutionStore;
import io.mohs.engine.JobStore;
import io.mohs.engine.StoredJob;
import io.mohs.jdbc.dialect.H2JdbcDialect;
import io.mohs.test.MutableClock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

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
        store = new JdbcExecutionStore(dataSource, clock, JsonMapper.builder().build(), new H2JdbcDialect());
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

    /** Insere ENQUEUED e transiciona pra RUNNING via SQL cru — {@link #complete} assume que já chegou lá. */
    private void seedRunningExecution(String id, String jobKey) {
        store.insert(execution(id, jobKey), new WelcomeEmail("a", 1));
        new JdbcTemplate(dataSource).update("UPDATE mohs_executions SET state = 'RUNNING' WHERE id = ?", id);
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

    @Test
    void completeTransitionsRunningExecutionAndRecordsTheAttempt() {
        seedRunningExecution("019abc-complete-1", "welcome-email");
        JobStore jobStore = new JdbcJobStore(dataSource, clock);
        Attempt attempt = new Attempt(1, clock.instant(), clock.instant(), ExecutionState.FAILED, "boom");

        boolean completed = store.complete(ExecutionId.of("019abc-complete-1"), JobKey.of("welcome-email"), attempt, ExecutionState.FAILED, jobStore);

        assertThat(completed).isTrue();
        Optional<Execution> found = store.find(ExecutionId.of("019abc-complete-1"));
        assertThat(found).isPresent();
        assertThat(found.get().state()).isEqualTo(ExecutionState.FAILED);
        assertThat(found.get().attempts()).containsExactly(attempt);
    }

    @Test
    void completeReturnsFalseAndWritesNothingWhenExecutionIsNotRunning() {
        Execution execution = execution("019abc-complete-2", "welcome-email"); // ENQUEUED, nunca chegou a RUNNING
        store.insert(execution, new WelcomeEmail("a", 1));
        JobStore jobStore = new JdbcJobStore(dataSource, clock);
        Attempt attempt = new Attempt(1, clock.instant(), clock.instant(), ExecutionState.FAILED, "boom");

        boolean completed = store.complete(execution.id(), execution.jobKey(), attempt, ExecutionState.FAILED, jobStore);

        assertThat(completed).isFalse();
        assertThat(store.find(execution.id())).contains(execution);
    }

    /** ADR-0024: CAS primeiro — uma segunda conclusão da mesma execução não sobrescreve nem duplica Attempt. */
    @Test
    void completeIsSafeUnderConcurrentConclusionOfTheSameExecution() {
        seedRunningExecution("019abc-complete-3", "welcome-email");
        JobStore jobStore = new JdbcJobStore(dataSource, clock);
        Attempt firstAttempt = new Attempt(1, clock.instant(), clock.instant(), ExecutionState.SUCCEEDED, null);
        Attempt secondAttempt = new Attempt(1, clock.instant(), clock.instant(), ExecutionState.FAILED, "boom");

        boolean first = store.complete(ExecutionId.of("019abc-complete-3"), JobKey.of("welcome-email"), firstAttempt, ExecutionState.SUCCEEDED, jobStore);
        boolean second = store.complete(ExecutionId.of("019abc-complete-3"), JobKey.of("welcome-email"), secondAttempt, ExecutionState.FAILED, jobStore);

        assertThat(first).isTrue();
        assertThat(second).isFalse();
        Optional<Execution> found = store.find(ExecutionId.of("019abc-complete-3"));
        assertThat(found.get().state()).isEqualTo(ExecutionState.SUCCEEDED);
        assertThat(found.get().attempts()).containsExactly(firstAttempt);
    }

    /** ADR-0025: liberar a vaga de concorrência é parte da mesma operação, não um passo separado que o chamador pode esquecer. */
    @Test
    void completeReleasesTheJobConcurrencySlot() {
        JdbcJobStore jobStore = new JdbcJobStore(dataSource, clock);
        jobStore.upsert(JobDefinition.of("report-summary", Handler.class, spec -> spec.onDemand().preventOverlap()));
        jobStore.tryIncrementRunningExecutions(JobKey.of("report-summary"));
        seedRunningExecution("019abc-complete-4", "report-summary");
        Attempt attempt = new Attempt(1, clock.instant(), clock.instant(), ExecutionState.FAILED, "boom");

        store.complete(ExecutionId.of("019abc-complete-4"), JobKey.of("report-summary"), attempt, ExecutionState.FAILED, jobStore);

        assertThat(jobStore.find(JobKey.of("report-summary"))).map(StoredJob::runningExecutionCount).contains(0);
    }

    /**
     * O trio (CAS de estado + INSERT do Attempt + decremento da vaga) é um
     * invariante que cruza duas tabelas — falha em qualquer passo desfaz
     * tudo. Sem a transação, um crash/erro depois do CAS deixava a execução
     * fora de RUNNING (invisível pro reaper) com running_execution_count
     * incrementado pra sempre: o mutex do job vazava permanentemente.
     */
    @Test
    void completeRollsBackTheStateTransitionWhenALaterStepFails() {
        seedRunningExecution("019abc-atomic-1", "welcome-email");
        JobStore failingJobStore = mock(JobStore.class);
        doThrow(new RuntimeException("simulated failure releasing the slot")).when(failingJobStore).decrementRunningExecutions(any());
        Attempt attempt = new Attempt(1, clock.instant(), clock.instant(), ExecutionState.FAILED, "boom");

        assertThatThrownBy(() -> store.complete(ExecutionId.of("019abc-atomic-1"), JobKey.of("welcome-email"), attempt, ExecutionState.FAILED, failingJobStore))
                .hasMessageContaining("simulated failure");

        Execution stillRunning = store.find(ExecutionId.of("019abc-atomic-1")).orElseThrow();
        assertThat(stillRunning.state()).isEqualTo(ExecutionState.RUNNING);
        assertThat(stillRunning.attempts()).isEmpty();
    }

    /** Mesma garantia de {@link #completeRollsBackTheStateTransitionWhenALaterStepFails}, pro caminho em lote do reaper. */
    @Test
    void completeAllRollsBackAllTransitionsWhenALaterStepFails() {
        seedRunningExecution("019abc-atomic-2", "welcome-email");
        seedRunningExecution("019abc-atomic-3", "welcome-email");
        JobStore failingJobStore = mock(JobStore.class);
        doThrow(new RuntimeException("simulated failure releasing the slot")).when(failingJobStore).decrementRunningExecutions(any());
        Attempt attempt = new Attempt(1, clock.instant(), clock.instant(), ExecutionState.FAILED, "boom");
        List<ExecutionStore.CompletionRequest> requests = List.of(
                new ExecutionStore.CompletionRequest(ExecutionId.of("019abc-atomic-2"), JobKey.of("welcome-email"), attempt, ExecutionState.FAILED),
                new ExecutionStore.CompletionRequest(ExecutionId.of("019abc-atomic-3"), JobKey.of("welcome-email"), attempt, ExecutionState.FAILED));

        assertThatThrownBy(() -> store.completeAll(requests, failingJobStore))
                .hasMessageContaining("simulated failure");

        assertThat(store.find(ExecutionId.of("019abc-atomic-2")).orElseThrow().state()).isEqualTo(ExecutionState.RUNNING);
        assertThat(store.find(ExecutionId.of("019abc-atomic-3")).orElseThrow().state()).isEqualTo(ExecutionState.RUNNING);
    }

    /** DBTUNE-14: mesma garantia de {@link #completeTransitionsRunningExecutionAndRecordsTheAttempt}, para várias execuções na mesma chamada. */
    @Test
    void completeAllTransitionsMultipleRunningExecutionsAndRecordsTheAttempts() {
        seedRunningExecution("019abc-completeall-1", "welcome-email");
        seedRunningExecution("019abc-completeall-2", "welcome-email");
        JobStore jobStore = new JdbcJobStore(dataSource, clock);
        Attempt attempt1 = new Attempt(1, clock.instant(), clock.instant(), ExecutionState.FAILED, "boom-1");
        Attempt attempt2 = new Attempt(1, clock.instant(), clock.instant(), ExecutionState.FAILED, "boom-2");
        List<ExecutionStore.CompletionRequest> requests = List.of(
                new ExecutionStore.CompletionRequest(ExecutionId.of("019abc-completeall-1"), JobKey.of("welcome-email"), attempt1, ExecutionState.FAILED),
                new ExecutionStore.CompletionRequest(ExecutionId.of("019abc-completeall-2"), JobKey.of("welcome-email"), attempt2, ExecutionState.FAILED));

        var completedIds = store.completeAll(requests, jobStore);

        assertThat(completedIds).containsExactlyInAnyOrder(ExecutionId.of("019abc-completeall-1"), ExecutionId.of("019abc-completeall-2"));
        assertThat(store.find(ExecutionId.of("019abc-completeall-1")).map(Execution::attempts)).contains(List.of(attempt1));
        assertThat(store.find(ExecutionId.of("019abc-completeall-2")).map(Execution::attempts)).contains(List.of(attempt2));
    }

    /** ADR-0024: mesma disciplina de CAS do {@link #complete} — no lote, cada request é independente. */
    @Test
    void completeAllExcludesRequestsForExecutionsThatAreNotRunning() {
        seedRunningExecution("019abc-completeall-3", "welcome-email");
        Execution stillEnqueued = execution("019abc-completeall-4", "welcome-email");
        store.insert(stillEnqueued, new WelcomeEmail("a", 1));
        JobStore jobStore = new JdbcJobStore(dataSource, clock);
        Attempt attempt3 = new Attempt(1, clock.instant(), clock.instant(), ExecutionState.FAILED, "boom-3");
        Attempt attempt4 = new Attempt(1, clock.instant(), clock.instant(), ExecutionState.FAILED, "boom-4");
        List<ExecutionStore.CompletionRequest> requests = List.of(
                new ExecutionStore.CompletionRequest(ExecutionId.of("019abc-completeall-3"), JobKey.of("welcome-email"), attempt3, ExecutionState.FAILED),
                new ExecutionStore.CompletionRequest(ExecutionId.of("019abc-completeall-4"), JobKey.of("welcome-email"), attempt4, ExecutionState.FAILED));

        var completedIds = store.completeAll(requests, jobStore);

        assertThat(completedIds).containsExactly(ExecutionId.of("019abc-completeall-3"));
        assertThat(store.find(ExecutionId.of("019abc-completeall-4"))).contains(stillEnqueued);
    }

    @Test
    void completeAllReleasesTheJobConcurrencySlotForEachCompletedExecution() {
        JdbcJobStore jobStore = new JdbcJobStore(dataSource, clock);
        jobStore.upsert(JobDefinition.of("report-summary", Handler.class, spec -> spec.onDemand().maxConcurrentExecutions(2)));
        jobStore.tryIncrementRunningExecutions(JobKey.of("report-summary"));
        jobStore.tryIncrementRunningExecutions(JobKey.of("report-summary"));
        seedRunningExecution("019abc-completeall-5", "report-summary");
        seedRunningExecution("019abc-completeall-6", "report-summary");
        Attempt attempt = new Attempt(1, clock.instant(), clock.instant(), ExecutionState.FAILED, "boom");
        List<ExecutionStore.CompletionRequest> requests = List.of(
                new ExecutionStore.CompletionRequest(ExecutionId.of("019abc-completeall-5"), JobKey.of("report-summary"), attempt, ExecutionState.FAILED),
                new ExecutionStore.CompletionRequest(ExecutionId.of("019abc-completeall-6"), JobKey.of("report-summary"), attempt, ExecutionState.FAILED));

        store.completeAll(requests, jobStore);

        assertThat(jobStore.find(JobKey.of("report-summary"))).map(StoredJob::runningExecutionCount).contains(0);
    }

    @Test
    void completeAllReturnsEmptySetForEmptyRequests() {
        assertThat(store.completeAll(List.of(), new JdbcJobStore(dataSource, clock))).isEmpty();
    }

    private static Execution executionAt(String id, String jobKey, Instant scheduledAt) {
        return new Execution(ExecutionId.of(id), JobKey.of(jobKey), ExecutionState.ENQUEUED, scheduledAt, null, List.of(), "application");
    }

    @Test
    void findPageOrdersByIdDescending() {
        store.insert(execution("019abc-page-1", "welcome-email"), new WelcomeEmail("a", 1));
        store.insert(execution("019abc-page-2", "welcome-email"), new WelcomeEmail("a", 1));
        store.insert(execution("019abc-page-3", "welcome-email"), new WelcomeEmail("a", 1));

        List<Execution> page = store.findPage(null, null, null, null, null, 10);

        assertThat(page).extracting(e -> e.id().value())
                .containsExactly("019abc-page-3", "019abc-page-2", "019abc-page-1");
    }

    @Test
    void findPageAppliesTheLimit() {
        store.insert(execution("019abc-limit-1", "welcome-email"), new WelcomeEmail("a", 1));
        store.insert(execution("019abc-limit-2", "welcome-email"), new WelcomeEmail("a", 1));

        List<Execution> page = store.findPage(null, null, null, null, null, 1);

        assertThat(page).extracting(e -> e.id().value()).containsExactly("019abc-limit-2");
    }

    @Test
    void findPageCursorExcludesIdsAtOrAfterIt() {
        store.insert(execution("019abc-cursor-1", "welcome-email"), new WelcomeEmail("a", 1));
        store.insert(execution("019abc-cursor-2", "welcome-email"), new WelcomeEmail("a", 1));
        store.insert(execution("019abc-cursor-3", "welcome-email"), new WelcomeEmail("a", 1));

        List<Execution> page = store.findPage(null, null, null, null, ExecutionId.of("019abc-cursor-3"), 10);

        assertThat(page).extracting(e -> e.id().value())
                .containsExactly("019abc-cursor-2", "019abc-cursor-1");
    }

    @Test
    void findPageFiltersByJobKey() {
        seedJobDefinition("other-job");
        store.insert(execution("019abc-filter-1", "welcome-email"), new WelcomeEmail("a", 1));
        store.insert(execution("019abc-filter-2", "other-job"), new WelcomeEmail("a", 1));

        List<Execution> page = store.findPage(JobKey.of("other-job"), null, null, null, null, 10);

        assertThat(page).extracting(e -> e.id().value()).containsExactly("019abc-filter-2");
    }

    @Test
    void findPageFiltersByStatus() {
        seedRunningExecution("019abc-status-1", "welcome-email");
        store.insert(execution("019abc-status-2", "welcome-email"), new WelcomeEmail("a", 1));

        List<Execution> page = store.findPage(null, ExecutionState.RUNNING, null, null, null, 10);

        assertThat(page).extracting(e -> e.id().value()).containsExactly("019abc-status-1");
    }

    @Test
    void findPageFiltersByScheduledAtRange() {
        store.insert(executionAt("019abc-range-1", "welcome-email", Instant.parse("2026-08-01T00:00:00Z")), new WelcomeEmail("a", 1));
        store.insert(executionAt("019abc-range-2", "welcome-email", Instant.parse("2026-08-10T00:00:00Z")), new WelcomeEmail("a", 1));
        store.insert(executionAt("019abc-range-3", "welcome-email", Instant.parse("2026-08-20T00:00:00Z")), new WelcomeEmail("a", 1));

        List<Execution> page = store.findPage(null, null,
                Instant.parse("2026-08-05T00:00:00Z"), Instant.parse("2026-08-15T00:00:00Z"), null, 10);

        assertThat(page).extracting(e -> e.id().value()).containsExactly("019abc-range-2");
    }
}
