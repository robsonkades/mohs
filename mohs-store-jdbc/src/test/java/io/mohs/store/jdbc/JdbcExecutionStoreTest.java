package io.mohs.store.jdbc;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.UnaryOperator;
import java.util.stream.IntStream;

import javax.sql.DataSource;

import org.h2.jdbcx.JdbcDataSource;
import org.jspecify.annotations.Nullable;
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
import io.mohs.engine.BatchCounters;
import io.mohs.engine.ExecutionStore;
import io.mohs.engine.JobStore;
import io.mohs.engine.StoredJob;
import io.mohs.store.jdbc.dialect.H2JdbcDialect;
import io.mohs.test.MutableClock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
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

    /** Insere ENQUEUED e leva a FAILED via SQL cru — o retry manual só reconhece esse estado. */
    private void seedFailedExecution(String id, String jobKey) {
        store.insert(execution(id, jobKey), new WelcomeEmail("a", 1));
        new JdbcTemplate(dataSource).update("UPDATE mohs_executions SET state = 'FAILED' WHERE id = ?", id);
    }

    /** ADR-0033/M3: retry manual rearma FAILED como RETRY_SCHEDULED com o scheduled_at reescrito — o claim faz o resto. */
    @Test
    void rearmForManualRetryFlipsAFailedExecution() {
        seedFailedExecution("019abc-retry-1", "welcome-email");
        Instant retryAt = clock.instant().plusSeconds(1);

        boolean armed = store.rearmForManualRetry(ExecutionId.of("019abc-retry-1"), retryAt);

        assertThat(armed).isTrue();
        Execution found = store.find(ExecutionId.of("019abc-retry-1")).orElseThrow();
        assertThat(found.state()).isEqualTo(ExecutionState.RETRY_SCHEDULED);
        assertThat(found.scheduledAt()).isEqualTo(retryAt);
    }

    /** Guard de retired no próprio CAS: job removido não rearma — a linha ficaria presa pra sempre (claim filtra retired, reaper só vê RUNNING — ADR-0033). */
    @Test
    void rearmForManualRetryRefusesAnExecutionOfARetiredJob() {
        seedFailedExecution("019abc-retry-3", "welcome-email");
        new JdbcTemplate(dataSource).update("UPDATE mohs_job_definitions SET retired = TRUE WHERE job_key = 'welcome-email'");

        boolean armed = store.rearmForManualRetry(ExecutionId.of("019abc-retry-3"), clock.instant().plusSeconds(1));

        assertThat(armed).isFalse();
        assertThat(store.find(ExecutionId.of("019abc-retry-3")).orElseThrow().state()).isEqualTo(ExecutionState.FAILED);
    }

    /** cancel_requested stale é limpo no rearme — retry manual é ordem mais nova do operador; sem a limpeza, o cancel antigo assassinaria o retry no primeiro tick. */
    @Test
    void rearmForManualRetryClearsAStaleCancelRequest() {
        seedFailedExecution("019abc-retry-4", "welcome-email");
        new JdbcTemplate(dataSource).update("UPDATE mohs_executions SET cancel_requested = TRUE WHERE id = '019abc-retry-4'");

        boolean armed = store.rearmForManualRetry(ExecutionId.of("019abc-retry-4"), clock.instant().plusSeconds(1));

        assertThat(armed).isTrue();
        Boolean flag = new JdbcTemplate(dataSource).queryForObject(
                "SELECT cancel_requested FROM mohs_executions WHERE id = '019abc-retry-4'", Boolean.class);
        assertThat(flag).isFalse();
    }

    /** Estado ≠ FAILED não é rearmável — RUNNING tem dono (o node executor), e o CAS devolve false sem tocar a linha. */
    @Test
    void rearmForManualRetryRefusesANonFailedExecution() {
        seedRunningExecution("019abc-retry-2", "welcome-email");
        Instant before = store.find(ExecutionId.of("019abc-retry-2")).orElseThrow().scheduledAt();

        boolean armed = store.rearmForManualRetry(ExecutionId.of("019abc-retry-2"), clock.instant().plusSeconds(1));

        assertThat(armed).isFalse();
        Execution found = store.find(ExecutionId.of("019abc-retry-2")).orElseThrow();
        assertThat(found.state()).isEqualTo(ExecutionState.RUNNING);
        assertThat(found.scheduledAt()).isEqualTo(before);
    }

    @Test
    void insertPersistsAndRoundTripsAnExecution() {
        Execution execution = execution("019abc-1", "welcome-email");

        store.insert(execution, new WelcomeEmail("ana", 31));
        Optional<Execution> found = store.find(ExecutionId.of("019abc-1"));

        assertThat(found).contains(execution);
    }

    /** Caracterização: o shape que o REST envia hoje pra job sem payload — Collections.unmodifiableMap — round-tripa? */
    @Test
    void findPayloadRoundTripsAnUnmodifiableMapPayload() {
        store.insert(execution("019abc-wrap", "welcome-email"),
                Collections.unmodifiableMap(new LinkedHashMap<String, Object>()));

        assertThat(store.findPayload(ExecutionId.of("019abc-wrap"))).contains(Map.of());
    }

    /**
     * ADR-0047: o veredito é por linha — a ilegível (classe fora do
     * classpath) sai em {@code unreadable} com a causa, as legíveis vêm
     * inteiras, e id inexistente simplesmente não aparece em mapa nenhum.
     */
    @Test
    void findPayloadsSeparatesReadableUnreadableAndMissingPerRow() {
        store.insert(execution("019abc-pb-1", "welcome-email"), new WelcomeEmail("ana", 31));
        store.insert(execution("019abc-pb-2", "welcome-email"), new WelcomeEmail("bia", 28));
        new JdbcTemplate(dataSource).update(
                "UPDATE mohs_executions SET payload_type = 'com.example.Gone' WHERE id = '019abc-pb-2'");

        ExecutionStore.PayloadBatch batch = store.findPayloads(List.of(
                ExecutionId.of("019abc-pb-1"), ExecutionId.of("019abc-pb-2"), ExecutionId.of("019abc-pb-missing")));

        assertThat(batch.payloads()).containsOnlyKeys(ExecutionId.of("019abc-pb-1"));
        assertThat(batch.payloads().get(ExecutionId.of("019abc-pb-1"))).isEqualTo(new WelcomeEmail("ana", 31));
        assertThat(batch.unreadable()).containsOnlyKeys(ExecutionId.of("019abc-pb-2"));
        assertThat(batch.unreadable().get(ExecutionId.of("019abc-pb-2"))).hasMessageContaining("com.example.Gone");
    }

    @Test
    void findPayloadsReturnsEmptyBatchForNoIds() {
        assertThat(store.findPayloads(List.of())).isEqualTo(ExecutionStore.PayloadBatch.EMPTY);
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

        LocalDateTime raw = new JdbcTemplate(dataSource).queryForObject(
                "SELECT scheduled_at FROM mohs_executions WHERE id = ?", LocalDateTime.class, "019abc-3");

        assertThat(raw).isEqualTo(LocalDateTime.ofInstant(scheduledAt, ZoneOffset.UTC));
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
        LocalDateTime startedAt = JdbcTimestamps.toUtcLocalDateTime(Instant.parse("2026-08-13T00:00:01Z"));
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

        boolean completed = store.complete(new ExecutionStore.CompletionRequest(ExecutionId.of("019abc-complete-1"), JobKey.of("welcome-email"), attempt, ExecutionState.FAILED), jobStore).applied();

        assertThat(completed).isTrue();
        Execution found = store.find(ExecutionId.of("019abc-complete-1")).orElseThrow();
        assertThat(found.state()).isEqualTo(ExecutionState.FAILED);
        assertThat(found.attempts()).containsExactly(attempt);
    }

    @Test
    void completeReturnsFalseAndWritesNothingWhenExecutionIsNotRunning() {
        Execution execution = execution("019abc-complete-2", "welcome-email"); // ENQUEUED, nunca chegou a RUNNING
        store.insert(execution, new WelcomeEmail("a", 1));
        JobStore jobStore = new JdbcJobStore(dataSource, clock);
        Attempt attempt = new Attempt(1, clock.instant(), clock.instant(), ExecutionState.FAILED, "boom");

        boolean completed = store.complete(new ExecutionStore.CompletionRequest(execution.id(), execution.jobKey(), attempt, ExecutionState.FAILED), jobStore).applied();

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

        boolean first = store.complete(new ExecutionStore.CompletionRequest(ExecutionId.of("019abc-complete-3"), JobKey.of("welcome-email"), firstAttempt, ExecutionState.SUCCEEDED), jobStore).applied();
        boolean second = store.complete(new ExecutionStore.CompletionRequest(ExecutionId.of("019abc-complete-3"), JobKey.of("welcome-email"), secondAttempt, ExecutionState.FAILED), jobStore).applied();

        assertThat(first).isTrue();
        assertThat(second).isFalse();
        Execution found = store.find(ExecutionId.of("019abc-complete-3")).orElseThrow();
        assertThat(found.state()).isEqualTo(ExecutionState.SUCCEEDED);
        assertThat(found.attempts()).containsExactly(firstAttempt);
    }

    /** ADR-0033: o backoff aterrissa junto do CAS — RETRY_SCHEDULED reescreve scheduled_at pra hora do retry na mesma transição. */
    @Test
    void completeToRetryScheduledRewritesScheduledAtToTheRetryTime() {
        seedRunningExecution("019abc-retry-1", "welcome-email");
        JobStore jobStore = new JdbcJobStore(dataSource, clock);
        Instant retryAt = clock.instant().plusSeconds(30);
        Attempt attempt = new Attempt(1, clock.instant(), clock.instant(), ExecutionState.FAILED, "boom");

        boolean completed = store.complete(new ExecutionStore.CompletionRequest(
                ExecutionId.of("019abc-retry-1"), JobKey.of("welcome-email"), attempt, ExecutionState.RETRY_SCHEDULED, retryAt), jobStore).applied();

        assertThat(completed).isTrue();
        Execution found = store.find(ExecutionId.of("019abc-retry-1")).orElseThrow();
        assertThat(found.state()).isEqualTo(ExecutionState.RETRY_SCHEDULED);
        assertThat(found.scheduledAt()).isEqualTo(retryAt);
        assertThat(found.attempts()).containsExactly(attempt);
    }

    /** Lote misto do reaper: exauridos viram FAILED, os com orçamento viram RETRY_SCHEDULED cada um com seu retryAt. */
    @Test
    void completeAllHandlesMixedTerminalAndRetryRequests() {
        seedRunningExecution("019abc-retry-2", "welcome-email");
        seedRunningExecution("019abc-retry-3", "welcome-email");
        JobStore jobStore = new JdbcJobStore(dataSource, clock);
        Instant retryAt = clock.instant().plusSeconds(45);
        Attempt failedAttempt = new Attempt(1, clock.instant(), clock.instant(), ExecutionState.FAILED, "exhausted");
        Attempt retryAttempt = new Attempt(1, clock.instant(), clock.instant(), ExecutionState.FAILED, "will retry");

        Map<ExecutionId, ExecutionStore.Completion> completed = store.completeAll(List.of(
                new ExecutionStore.CompletionRequest(ExecutionId.of("019abc-retry-2"), JobKey.of("welcome-email"), failedAttempt, ExecutionState.FAILED),
                new ExecutionStore.CompletionRequest(ExecutionId.of("019abc-retry-3"), JobKey.of("welcome-email"), retryAttempt, ExecutionState.RETRY_SCHEDULED, retryAt)),
                jobStore);

        assertThat(completed).containsOnlyKeys(ExecutionId.of("019abc-retry-2"), ExecutionId.of("019abc-retry-3"));
        assertThat(completed.values()).allMatch(ExecutionStore.Completion::applied);
        assertThat(store.find(ExecutionId.of("019abc-retry-2")).orElseThrow().state()).isEqualTo(ExecutionState.FAILED);
        Execution retried = store.find(ExecutionId.of("019abc-retry-3")).orElseThrow();
        assertThat(retried.state()).isEqualTo(ExecutionState.RETRY_SCHEDULED);
        assertThat(retried.scheduledAt()).isEqualTo(retryAt);
    }

    /** Fence anti-ABA (ADR-0051): se a posse observada — (node_id, fired_at) — já não é a da linha (re-claim concorrente gravou nova encarnação), o CAS perde — a encarnação nova saudável nunca é morta. */
    @Test
    void completeWithAStaleOwnerFenceLosesTheCas() {
        seedRunningExecution("019abc-fence-1", "welcome-email");
        Instant currentFiredAt = clock.instant();
        new JdbcTemplate(dataSource).update("UPDATE mohs_executions SET node_id = 'node-b', fired_at = ? WHERE id = ?",
                JdbcTimestamps.toUtcLocalDateTime(currentFiredAt), "019abc-fence-1");
        JobStore jobStore = new JdbcJobStore(dataSource, clock);
        Attempt attempt = new Attempt(1, clock.instant(), clock.instant(), ExecutionState.FAILED, "lease expired");
        // a posse antiga: mesmo id de node diferente OU fired_at anterior — aqui as duas coisas
        ExecutionStore.OwnerFence staleFence = new ExecutionStore.OwnerFence("node-a", currentFiredAt.minusSeconds(60));

        boolean completed = store.complete(new ExecutionStore.CompletionRequest(
                ExecutionId.of("019abc-fence-1"), JobKey.of("welcome-email"), attempt, ExecutionState.FAILED, null, staleFence), jobStore).applied();

        assertThat(completed).isFalse();
        Execution untouched = store.find(ExecutionId.of("019abc-fence-1")).orElseThrow();
        assertThat(untouched.state()).isEqualTo(ExecutionState.RUNNING);
        assertThat(untouched.attempts()).isEmpty();
    }

    @Test
    void completeWithTheMatchingOwnerFenceWins() {
        seedRunningExecution("019abc-fence-2", "welcome-email");
        Instant firedAt = clock.instant().minusSeconds(5);
        new JdbcTemplate(dataSource).update("UPDATE mohs_executions SET node_id = 'node-a', fired_at = ? WHERE id = ?",
                JdbcTimestamps.toUtcLocalDateTime(firedAt), "019abc-fence-2");
        JobStore jobStore = new JdbcJobStore(dataSource, clock);
        Attempt attempt = new Attempt(1, clock.instant(), clock.instant(), ExecutionState.FAILED, "lease expired");

        boolean completed = store.complete(new ExecutionStore.CompletionRequest(
                ExecutionId.of("019abc-fence-2"), JobKey.of("welcome-email"), attempt, ExecutionState.FAILED, null,
                new ExecutionStore.OwnerFence("node-a", firedAt)), jobStore).applied();

        assertThat(completed).isTrue();
        assertThat(store.find(ExecutionId.of("019abc-fence-2")).orElseThrow().state()).isEqualTo(ExecutionState.FAILED);
    }

    /** ADR-0034: cancel de execução que ainda não roda é CAS direto pra CANCELLED — só os dois estados claimáveis; RUNNING fica pro caminho da flag. */
    @Test
    void cancelIfPendingCancelsOnlyClaimableStates() {
        JdbcTemplate raw = new JdbcTemplate(dataSource);
        store.insert(execution("019abc-cancel-1", "welcome-email"), new WelcomeEmail("a", 1));
        seedRunningExecution("019abc-cancel-2", "welcome-email");
        store.insert(execution("019abc-cancel-3", "welcome-email"), new WelcomeEmail("a", 1));
        raw.update("UPDATE mohs_executions SET state = 'RETRY_SCHEDULED' WHERE id = '019abc-cancel-3'");

        assertThat(store.cancelIfPending(ExecutionId.of("019abc-cancel-1"))).isTrue();
        assertThat(store.cancelIfPending(ExecutionId.of("019abc-cancel-2"))).isFalse();
        assertThat(store.cancelIfPending(ExecutionId.of("019abc-cancel-3"))).isTrue();
        assertThat(store.find(ExecutionId.of("019abc-cancel-1")).orElseThrow().state()).isEqualTo(ExecutionState.CANCELLED);
        assertThat(store.find(ExecutionId.of("019abc-cancel-2")).orElseThrow().state()).isEqualTo(ExecutionState.RUNNING);
        assertThat(store.find(ExecutionId.of("019abc-cancel-3")).orElseThrow().state()).isEqualTo(ExecutionState.CANCELLED);
    }

    /** ADR-0034: a flag só alcança RUNNING — pendente cancela por CAS, terminal já decidiu. */
    @Test
    void requestCancellationFlagsOnlyRunningExecutions() {
        seedRunningExecution("019abc-flag-1", "welcome-email");
        store.insert(execution("019abc-flag-2", "welcome-email"), new WelcomeEmail("a", 1));

        assertThat(store.requestCancellation(ExecutionId.of("019abc-flag-1"))).isTrue();
        assertThat(store.requestCancellation(ExecutionId.of("019abc-flag-2"))).isFalse();
        JdbcTemplate raw = new JdbcTemplate(dataSource);
        assertThat(raw.queryForObject("SELECT cancel_requested FROM mohs_executions WHERE id = '019abc-flag-1'", Boolean.class)).isTrue();
        assertThat(raw.queryForObject("SELECT cancel_requested FROM mohs_executions WHERE id = '019abc-flag-2'", Boolean.class)).isFalse();
    }

    /** O poll do tick (ADR-0034): em lote, devolve só os ids com a flag ligada. */
    @Test
    void findCancelRequestedReturnsOnlyFlaggedIds() {
        seedRunningExecution("019abc-poll-1", "welcome-email");
        seedRunningExecution("019abc-poll-2", "welcome-email");
        store.requestCancellation(ExecutionId.of("019abc-poll-1"));

        assertThat(store.findCancelRequested(List.of(ExecutionId.of("019abc-poll-1"), ExecutionId.of("019abc-poll-2"))))
                .containsExactly(ExecutionId.of("019abc-poll-1"));
        assertThat(store.findCancelRequested(List.of())).isEmpty();
    }

    /**
     * Fallback do {@code SUCCESS_NO_INFO} (o JDBC permite ao driver executar
     * o batch sem contar linhas): a confirmação do {@code completeAll} recai
     * no SELECT por estado. O decorator reproduz o contrato exato do driver:
     * executa o batch de verdade e só mente a contagem. O tracker prova que
     * a interceptação aconteceu — sem ele, uma mudança interna do spring-jdbc
     * (ex.: migrar pra {@code executeLargeBatch}) faria o teste decair em
     * silêncio pro caminho normal, cobertura fantasma do branch.
     */
    @Test
    void completeAllFallsBackToStateSelectWhenTheDriverReturnsSuccessNoInfo() {
        JdbcTemplate raw = new JdbcTemplate(dataSource);
        seedRunningExecution("019abc-noinfo-1", "welcome-email");
        seedRunningExecution("019abc-noinfo-2", "welcome-email");
        // o CAS do segundo perde de propósito (a linha já saiu de RUNNING) —
        // prova que o SELECT de confirmação discrimina, não confirma o lote inteiro
        raw.update("UPDATE mohs_executions SET state = 'RETRY_SCHEDULED' WHERE id = '019abc-noinfo-2'");
        AtomicBoolean rewroteCounts = new AtomicBoolean();
        JdbcExecutionStore noInfoStore = new JdbcExecutionStore(
                successNoInfoDataSource(dataSource, rewroteCounts), clock, JsonMapper.builder().build(), new H2JdbcDialect());
        JobStore jobStore = new JdbcJobStore(dataSource, clock);
        Attempt attempt = new Attempt(1, clock.instant(), clock.instant(), ExecutionState.SUCCEEDED, null);

        Map<ExecutionId, ExecutionStore.Completion> verdicts = noInfoStore.completeAll(List.of(
                new ExecutionStore.CompletionRequest(ExecutionId.of("019abc-noinfo-1"), JobKey.of("welcome-email"), attempt, ExecutionState.SUCCEEDED),
                new ExecutionStore.CompletionRequest(ExecutionId.of("019abc-noinfo-2"), JobKey.of("welcome-email"), attempt, ExecutionState.SUCCEEDED)),
                jobStore);

        assertThat(rewroteCounts)
                .as("the decorator must have intercepted executeBatch — otherwise this exercises the normal path, not the fallback")
                .isTrue();
        assertThat(verdicts.get(ExecutionId.of("019abc-noinfo-1")).applied()).isTrue();
        assertThat(verdicts.get(ExecutionId.of("019abc-noinfo-2")).applied()).isFalse();
        assertThat(store.find(ExecutionId.of("019abc-noinfo-1")).orElseThrow().state()).isEqualTo(ExecutionState.SUCCEEDED);
        assertThat(store.find(ExecutionId.of("019abc-noinfo-2")).orElseThrow().state()).isEqualTo(ExecutionState.RETRY_SCHEDULED);
    }

    private static DataSource successNoInfoDataSource(DataSource delegate, AtomicBoolean rewroteCounts) {
        return proxyOf(DataSource.class, delegate,
                result -> result instanceof Connection connection ? successNoInfoConnection(connection, rewroteCounts) : result);
    }

    private static Connection successNoInfoConnection(Connection delegate, AtomicBoolean rewroteCounts) {
        return proxyOf(Connection.class, delegate,
                result -> result instanceof PreparedStatement statement ? successNoInfoStatement(statement, rewroteCounts) : result);
    }

    /** Só {@code executeBatch()} devolve {@code int[]} — reescrever as contagens ali é o {@code SUCCESS_NO_INFO} do driver, com o batch já aplicado. */
    private static PreparedStatement successNoInfoStatement(PreparedStatement delegate, AtomicBoolean rewroteCounts) {
        return proxyOf(PreparedStatement.class, delegate, result -> {
            if (result instanceof int[] counts) {
                Arrays.fill(counts, Statement.SUCCESS_NO_INFO);
                rewroteCounts.set(true);
            }
            return result;
        });
    }

    private static <T> T proxyOf(Class<T> type, T delegate, UnaryOperator<Object> wrapResult) {
        InvocationHandler handler = (proxy, method, args) -> {
            try {
                return wrapResult.apply(method.invoke(delegate, args));
            } catch (InvocationTargetException e) {
                throw e.getCause();
            }
        };
        return type.cast(Proxy.newProxyInstance(JdbcExecutionStoreTest.class.getClassLoader(), new Class<?>[]{type}, handler));
    }

    /** As duas combinações inválidas são bug do chamador — rejeitadas na construção, nunca gravadas pela metade. */
    @Test
    void completionRequestRejectsRetryAtStateMismatches() {
        Attempt attempt = new Attempt(1, clock.instant(), clock.instant(), ExecutionState.FAILED, "boom");

        assertThatThrownBy(() -> new ExecutionStore.CompletionRequest(
                ExecutionId.of("x"), JobKey.of("welcome-email"), attempt, ExecutionState.RETRY_SCHEDULED, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("requires retryAt");
        assertThatThrownBy(() -> new ExecutionStore.CompletionRequest(
                ExecutionId.of("x"), JobKey.of("welcome-email"), attempt, ExecutionState.FAILED, clock.instant()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("only applies to RETRY_SCHEDULED");
    }

    /** ADR-0035: rearme fixed-delay só em estado terminal — com retry a corrente ainda está viva. */
    @Test
    void completionRequestRejectsRearmOnARetry() {
        Attempt attempt = new Attempt(1, clock.instant(), clock.instant(), ExecutionState.FAILED, "boom");

        assertThatThrownBy(() -> new ExecutionStore.CompletionRequest(
                ExecutionId.of("x"), JobKey.of("welcome-email"), attempt, ExecutionState.RETRY_SCHEDULED,
                clock.instant(), null, clock.instant()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("rearmNextFireAt");
    }

    /** ADR-0035: o rearme da corrente fixed-delay aterrissa na mesma transação do CAS de conclusão. */
    @Test
    void completeRearmsAnUnarmedTriggerWhenRequested() {
        JdbcJobStore jobStore = new JdbcJobStore(dataSource, clock);
        disarmTrigger("welcome-email");
        seedRunningExecution("019abc-rearm-1", "welcome-email");
        Attempt attempt = new Attempt(1, clock.instant(), clock.instant(), ExecutionState.SUCCEEDED, null);
        Instant rearmAt = clock.instant().plusSeconds(300);

        boolean completed = store.complete(new ExecutionStore.CompletionRequest(
                ExecutionId.of("019abc-rearm-1"), JobKey.of("welcome-email"), attempt, ExecutionState.SUCCEEDED,
                null, null, rearmAt), jobStore).applied();

        assertThat(completed).isTrue();
        assertThat(nextFireAtOf("welcome-email")).isEqualTo(rearmAt);
    }

    /** Guard IS NULL do rearme: um trigger já armado (upsert de mudança de agenda no meio do voo) nunca é clobrado pela conclusão. */
    @Test
    void completeDoesNotOverwriteAnAlreadyArmedTrigger() {
        JdbcJobStore jobStore = new JdbcJobStore(dataSource, clock);
        Instant armedAt = clock.instant().plusSeconds(60);
        new JdbcTemplate(dataSource).update("UPDATE mohs_job_definitions SET next_fire_at = ? WHERE job_key = ?",
                JdbcTimestamps.toUtcLocalDateTime(armedAt), "welcome-email");
        seedRunningExecution("019abc-rearm-2", "welcome-email");
        Attempt attempt = new Attempt(1, clock.instant(), clock.instant(), ExecutionState.SUCCEEDED, null);

        store.complete(new ExecutionStore.CompletionRequest(
                ExecutionId.of("019abc-rearm-2"), JobKey.of("welcome-email"), attempt, ExecutionState.SUCCEEDED,
                null, null, clock.instant().plusSeconds(300)), jobStore);

        assertThat(nextFireAtOf("welcome-email")).isEqualTo(armedAt);
    }

    /** O caminho em lote do reaper rearma do mesmo jeito que o unitário. */
    @Test
    void completeAllRearmsWhenRequested() {
        JdbcJobStore jobStore = new JdbcJobStore(dataSource, clock);
        disarmTrigger("welcome-email");
        seedRunningExecution("019abc-rearm-3", "welcome-email");
        Attempt attempt = new Attempt(1, clock.instant(), clock.instant(), ExecutionState.FAILED, "boom");
        Instant rearmAt = clock.instant().plusSeconds(300);

        store.completeAll(List.of(new ExecutionStore.CompletionRequest(
                ExecutionId.of("019abc-rearm-3"), JobKey.of("welcome-email"), attempt, ExecutionState.FAILED,
                null, null, rearmAt)), jobStore);

        assertThat(nextFireAtOf("welcome-email")).isEqualTo(rearmAt);
    }

    private void disarmTrigger(String jobKey) {
        new JdbcTemplate(dataSource).update("UPDATE mohs_job_definitions SET next_fire_at = NULL WHERE job_key = ?", jobKey);
    }

    private @Nullable Instant nextFireAtOf(String jobKey) {
        LocalDateTime stored = new JdbcTemplate(dataSource).queryForObject(
                "SELECT next_fire_at FROM mohs_job_definitions WHERE job_key = ?", LocalDateTime.class, jobKey);
        return stored == null ? null : JdbcTimestamps.fromUtcLocalDateTime(stored);
    }

    /** ADR-0025: liberar a vaga de concorrência é parte da mesma operação, não um passo separado que o chamador pode esquecer. */
    @Test
    void completeReleasesTheJobConcurrencySlot() {
        JdbcJobStore jobStore = new JdbcJobStore(dataSource, clock);
        jobStore.upsert(JobDefinition.of("report-summary", Handler.class, spec -> spec.onDemand().preventOverlap()));
        jobStore.tryIncrementRunningExecutions(JobKey.of("report-summary"));
        seedRunningExecution("019abc-complete-4", "report-summary");
        Attempt attempt = new Attempt(1, clock.instant(), clock.instant(), ExecutionState.FAILED, "boom");

        store.complete(new ExecutionStore.CompletionRequest(ExecutionId.of("019abc-complete-4"), JobKey.of("report-summary"), attempt, ExecutionState.FAILED), jobStore);

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

        assertThatThrownBy(() -> store.complete(new ExecutionStore.CompletionRequest(ExecutionId.of("019abc-atomic-1"), JobKey.of("welcome-email"), attempt, ExecutionState.FAILED), failingJobStore))
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
        // a sobrecarga em bloco (ADR-0047) é a que o completeAll usa agora
        doThrow(new RuntimeException("simulated failure releasing the slot")).when(failingJobStore).decrementRunningExecutions(any(), anyInt());
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

        var verdicts = store.completeAll(requests, jobStore);

        assertThat(verdicts).containsOnlyKeys(ExecutionId.of("019abc-completeall-1"), ExecutionId.of("019abc-completeall-2"));
        assertThat(verdicts.values()).allMatch(ExecutionStore.Completion::applied);
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

        var verdicts = store.completeAll(requests, jobStore);

        // ADR-0047: todo request tem veredito — o perdedor vem NOT_APPLIED, nunca ausente
        assertThat(verdicts).containsOnlyKeys(ExecutionId.of("019abc-completeall-3"), ExecutionId.of("019abc-completeall-4"));
        assertThat(verdicts.get(ExecutionId.of("019abc-completeall-3")).applied()).isTrue();
        assertThat(verdicts.get(ExecutionId.of("019abc-completeall-4"))).isEqualTo(ExecutionStore.Completion.NOT_APPLIED);
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
    void completeAllReturnsEmptyMapForEmptyRequests() {
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

    /** DBTUNE-21: página é SUMÁRIO — attempts existentes NÃO são hidratados (pertencem ao detalhe {@code find}); listagem de dashboard não paga a segunda query nem carrega {@code error} de tamanho arbitrário. */
    @Test
    void findPageIsASummaryAndDoesNotHydrateAttempts() {
        store.insert(execution("019abc-summary-1", "welcome-email"), new WelcomeEmail("a", 1));
        insertAttempt("019abc-summary-1", 1, clock.instant(), "SUCCEEDED");

        List<Execution> page = store.findPage(null, null, null, null, null, 10);

        assertThat(page).extracting(e -> e.id().value()).containsExactly("019abc-summary-1");
        assertThat(page.getFirst().attempts()).isEmpty();
        assertThat(store.find(ExecutionId.of("019abc-summary-1")).orElseThrow().attempts()).hasSize(1);
    }

    /** GET /overview: só o trabalho vivo conta — terminal fica de fora por contrato; RUNNING carrega o predicado da lease (DBTUNE-17), que o claim sempre grava na mesma escrita. */
    @Test
    void countActiveByStateCountsOnlyLiveWork() {
        store.insert(execution("019abc-count-1", "welcome-email"), new WelcomeEmail("a", 1));
        store.insert(execution("019abc-count-2", "welcome-email"), new WelcomeEmail("a", 1));
        seedRunningExecution("019abc-count-3", "welcome-email");
        new JdbcTemplate(dataSource).update("UPDATE mohs_executions SET lease_expires_at = ? WHERE id = ?",
                JdbcTimestamps.toUtcLocalDateTime(clock.instant().plusSeconds(30)), "019abc-count-3");
        store.insert(execution("019abc-count-4", "welcome-email"), new WelcomeEmail("a", 1));
        new JdbcTemplate(dataSource).update("UPDATE mohs_executions SET state = 'RETRY_SCHEDULED' WHERE id = ?", "019abc-count-4");
        seedFailedExecution("019abc-count-5", "welcome-email");

        assertThat(store.countActiveByState()).containsOnly(
                entry(ExecutionState.ENQUEUED, 2L),
                entry(ExecutionState.RUNNING, 1L),
                entry(ExecutionState.RETRY_SCHEDULED, 1L));
    }

    /** GET /overview: a vazão conta só desfechos terminais DENTRO da janela — RETRY_SCHEDULED não é desfecho (a execução segue viva no backlog) e attempt sem finished_at fica fora por construção. */
    @Test
    void countTerminalOutcomesSinceCountsOnlyWindowedTerminalAttempts() {
        store.insert(execution("019abc-tput-1", "welcome-email"), new WelcomeEmail("a", 1));
        Instant since = clock.instant().minusSeconds(60);
        insertAttempt("019abc-tput-1", 1, since.minusSeconds(1), "SUCCEEDED");
        insertAttempt("019abc-tput-1", 2, since, "SUCCEEDED");
        insertAttempt("019abc-tput-1", 3, since.plusSeconds(10), "SUCCEEDED");
        insertAttempt("019abc-tput-1", 4, since.plusSeconds(20), "FAILED");
        insertAttempt("019abc-tput-1", 5, since.plusSeconds(30), "RETRY_SCHEDULED");
        insertAttempt("019abc-tput-1", 6, null, "FAILED");

        assertThat(store.countTerminalOutcomesSince(since)).containsOnly(
                entry(ExecutionState.SUCCEEDED, 2L),
                entry(ExecutionState.FAILED, 1L));
    }

    private void insertAttempt(String executionId, int number, @Nullable Instant finishedAt, String outcome) {
        new JdbcTemplate(dataSource).update("""
                INSERT INTO mohs_attempts (execution_id, number, started_at, finished_at, outcome)
                VALUES (?, ?, ?, ?, ?)
                """, executionId, number, JdbcTimestamps.toUtcLocalDateTime(clock.instant().minusSeconds(120)),
                finishedAt == null ? null : JdbcTimestamps.toUtcLocalDateTime(finishedAt), outcome);
    }

    /**
     * ADR-0043: o lote fecha na conclusão do ÚLTIMO membro, e só nela. É essa
     * conclusão que carrega o saldo de volta, porque é ela que vai publicar
     * {@code BatchCompleted} — relido depois, o saldo seria o mesmo para as
     * duas conclusões e as duas se achariam a fechadora. CANCELLED e as demais
     * terminais não-SUCCEEDED contam como falha: o lote responde quantos deram
     * certo, não por que os outros não deram.
     */
    @Test
    void onlyTheCompletionThatEmptiesTheBatchCarriesItsCounters() {
        JobStore jobStore = new JdbcJobStore(dataSource, clock);
        new JdbcBatchStore(dataSource, clock).insert("b1", 2);
        seedBatchMember("019abc-member-1", "b1");
        seedBatchMember("019abc-member-2", "b1");

        BatchCounters afterFirst = completeMember("019abc-member-1", "b1", ExecutionState.SUCCEEDED, jobStore);
        BatchCounters afterLast = completeMember("019abc-member-2", "b1", ExecutionState.FAILED, jobStore);

        assertThat(afterFirst).isNull();
        assertThat(afterLast).isNotNull();
        assertThat(afterLast.succeeded()).isEqualTo(1);
        assertThat(afterLast.failed()).isEqualTo(1);
        assertThat(afterLast.pending()).isZero();
    }

    /** Retry nao conta: o membro continua vivo, e conta-lo fecharia o lote antes da hora. */
    @Test
    void aScheduledRetryDoesNotCountTowardTheBatch() {
        JobStore jobStore = new JdbcJobStore(dataSource, clock);
        JdbcBatchStore batches = new JdbcBatchStore(dataSource, clock);
        batches.insert("b2", 1);
        seedBatchMember("019abc-member-3", "b2");

        Attempt attempt = new Attempt(1, clock.instant(), clock.instant(), ExecutionState.FAILED, "boom");
        ExecutionStore.Completion completion = store.complete(new ExecutionStore.CompletionRequest(
                ExecutionId.of("019abc-member-3"), JobKey.of("welcome-email"), attempt,
                ExecutionState.RETRY_SCHEDULED, clock.instant()).inBatch("b2"), jobStore);

        assertThat(completion.applied()).isTrue();
        assertThat(completion.closedBatch()).isNull();
        BatchCounters counters = batches.find("b2").orElseThrow();
        assertThat(counters.succeeded()).isZero();
        assertThat(counters.failed()).isZero();
        assertThat(counters.pending()).isEqualTo(1);
    }

    private void seedBatchMember(String id, String batchId) {
        seedRunningExecution(id, "welcome-email");
        new JdbcTemplate(dataSource).update("UPDATE mohs_executions SET batch_id = ? WHERE id = ?", batchId, id);
    }

    private @Nullable BatchCounters completeMember(String id, String batchId, ExecutionState outcome, JobStore jobStore) {
        Attempt attempt = new Attempt(1, clock.instant(), clock.instant(), outcome,
                outcome == ExecutionState.SUCCEEDED ? null : "boom");
        return store.complete(new ExecutionStore.CompletionRequest(ExecutionId.of(id), JobKey.of("welcome-email"),
                attempt, outcome).inBatch(batchId), jobStore).closedBatch();
    }

    /**
     * O caminho do reaper conta igual ao do dispatcher. Sem isto, um membro
     * que morre junto com o nó some do lote: {@code pending} nunca zera,
     * ninguém fecha, e a ADR-0043 dispensou a varredura de reconciliação
     * justamente sob a premissa de que todo caminho conta. O retry no mesmo
     * lote continua não contando — ele ainda está vivo.
     */
    @Test
    void completeAllCountsBatchMembersToo() {
        JobStore jobStore = new JdbcJobStore(dataSource, clock);
        JdbcBatchStore batches = new JdbcBatchStore(dataSource, clock);
        batches.insert("b3", 3);
        seedBatchMember("019abc-reclaim-1", "b3");
        seedBatchMember("019abc-reclaim-2", "b3");
        seedBatchMember("019abc-reclaim-3", "b3");
        Attempt attempt = new Attempt(1, clock.instant(), clock.instant(), ExecutionState.FAILED, "lease expired");

        store.completeAll(List.of(
                new ExecutionStore.CompletionRequest(ExecutionId.of("019abc-reclaim-1"), JobKey.of("welcome-email"),
                        attempt, ExecutionState.FAILED).inBatch("b3"),
                new ExecutionStore.CompletionRequest(ExecutionId.of("019abc-reclaim-2"), JobKey.of("welcome-email"),
                        attempt, ExecutionState.CANCELLED).inBatch("b3"),
                new ExecutionStore.CompletionRequest(ExecutionId.of("019abc-reclaim-3"), JobKey.of("welcome-email"),
                        attempt, ExecutionState.RETRY_SCHEDULED, clock.instant().plusSeconds(30)).inBatch("b3")),
                jobStore);

        BatchCounters counters = batches.find("b3").orElseThrow();
        assertThat(counters.failed()).isEqualTo(2);
        assertThat(counters.succeeded()).isZero();
        assertThat(counters.pending()).isEqualTo(1);
    }

    /**
     * ADR-0047: o {@code completeAll} agora carrega o veredito de fechador
     * no retorno — antes ele contava e DESCARTAVA (o wart do item 2 do
     * BATCH-ARCHITECTURE-REVIEW); com o group commit, quem publica
     * {@code BatchCompleted} lê daqui. Exatamente UM request do lote
     * fechado recebe os counters, mesmo com os dois no mesmo flush.
     */
    @Test
    void completeAllElectsExactlyOneBatchCloserInItsVerdicts() {
        JobStore jobStore = new JdbcJobStore(dataSource, clock);
        JdbcBatchStore batches = new JdbcBatchStore(dataSource, clock);
        batches.insert("b5", 2);
        seedBatchMember("019abc-closer-1", "b5");
        seedBatchMember("019abc-closer-2", "b5");
        Attempt attempt = new Attempt(1, clock.instant(), clock.instant(), ExecutionState.SUCCEEDED, null);

        var verdicts = store.completeAll(List.of(
                new ExecutionStore.CompletionRequest(ExecutionId.of("019abc-closer-1"), JobKey.of("welcome-email"),
                        attempt, ExecutionState.SUCCEEDED).inBatch("b5"),
                new ExecutionStore.CompletionRequest(ExecutionId.of("019abc-closer-2"), JobKey.of("welcome-email"),
                        attempt, ExecutionState.SUCCEEDED).inBatch("b5")),
                jobStore);

        assertThat(verdicts.values()).allMatch(ExecutionStore.Completion::applied);
        List<BatchCounters> closers = verdicts.values().stream()
                .map(ExecutionStore.Completion::closedBatch)
                .filter(Objects::nonNull)
                .toList();
        assertThat(closers).hasSize(1);
        assertThat(closers.get(0).succeeded()).isEqualTo(2);
        assertThat(closers.get(0).pending()).isZero();
    }

    /**
     * Cancelar um membro pendente é um fim como outro qualquer: se não contar,
     * o lote nunca fecha e não há reconciliação que cure (ADR-0043). Alcançável
     * por POST /executions/{id}/cancel, então é caminho de operador, não corrida.
     */
    @Test
    void cancellingAPendingMemberCountsItIntoTheBatch() {
        JdbcBatchStore batches = new JdbcBatchStore(dataSource, clock);
        batches.insert("b4", 1);
        store.insert(execution("019abc-cancel-1", "welcome-email"), new WelcomeEmail("ana", 31));
        new JdbcTemplate(dataSource).update("UPDATE mohs_executions SET batch_id = ? WHERE id = ?", "b4", "019abc-cancel-1");

        assertThat(store.cancelIfPending(ExecutionId.of("019abc-cancel-1"))).isTrue();

        BatchCounters counters = batches.find("b4").orElseThrow();
        assertThat(counters.failed()).isEqualTo(1);
        assertThat(counters.pending()).isZero();
    }

    /**
     * Membro de lote não entra no retry manual (ADR-0043): a falha dele já foi
     * contada, e recontá-la fecharia o lote antes da hora — ou exigiria reabrir
     * o lote, o que faria BatchCompleted deixar de ser terminal. A guarda mora
     * no CAS, não no chamador, porque é o CAS que decide se a linha muda.
     */
    @Test
    void aBatchMemberIsNotRearmedForManualRetry() {
        new JdbcBatchStore(dataSource, clock).insert("b5", 1);
        seedFailedExecution("019abc-retry-batch", "welcome-email");
        new JdbcTemplate(dataSource).update("UPDATE mohs_executions SET batch_id = ? WHERE id = ?", "b5", "019abc-retry-batch");

        boolean rearmed = store.rearmForManualRetry(ExecutionId.of("019abc-retry-batch"), clock.instant());

        assertThat(rearmed).isFalse();
        assertThat(store.find(ExecutionId.of("019abc-retry-batch")).orElseThrow().state())
                .isEqualTo(ExecutionState.FAILED);
    }
}
