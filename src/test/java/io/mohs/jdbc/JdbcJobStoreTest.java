package io.mohs.jdbc;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import javax.sql.DataSource;

import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

import io.mohs.core.definition.DefinitionSource;
import io.mohs.core.definition.JobDefinition;
import io.mohs.core.definition.PolicySpec;
import io.mohs.core.job.JobKey;
import io.mohs.core.schedule.CronSpec;
import io.mohs.core.schedule.IntervalSpec;
import io.mohs.core.schedule.Misfire;
import io.mohs.core.schedule.OnDemandSpec;
import io.mohs.core.schedule.Schedule;
import io.mohs.engine.StoredJob;
import io.mohs.test.MutableClock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JdbcJobStoreTest {

    record Handler() {
    }

    private DataSource dataSource;
    private MutableClock clock;
    private JdbcJobStore store;

    @BeforeEach
    void setUp() {
        dataSource = freshH2DataSource();
        clock = new MutableClock(Instant.parse("2026-08-13T00:00:00Z"), ZoneId.of("UTC"));
        store = new JdbcJobStore(dataSource, clock);
    }

    private static DataSource freshH2DataSource() {
        JdbcDataSource h2 = new JdbcDataSource();
        h2.setURL("jdbc:h2:mem:job-store-test-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        h2.setUser("sa");
        h2.setPassword("");
        new ResourceDatabasePopulator(new ClassPathResource("schema-h2.sql")).execute(h2);
        return h2;
    }

    private static JobDefinition definition(String id, Schedule schedule) {
        return JobDefinition.of(id, Handler.class, spec -> {
            PolicySpec policySpec = switch (schedule) {
                case CronSpec cron -> spec.cron(cron.expression(), cron.zone());
                case IntervalSpec interval -> interval.afterFinish()
                        ? spec.everyAfterFinish(interval.interval())
                        : spec.every(interval.interval());
                case OnDemandSpec onDemand -> spec.onDemand();
            };
            policySpec.runner("io").retries(3);
        });
    }

    private static JobDefinition definitionWithCap(String id, int max) {
        return JobDefinition.of(id, Handler.class, spec -> spec.onDemand().maxConcurrentExecutions(max));
    }

    /** JobDefinition.of (builder público) hardcoda PROGRAMMATIC — só o construtor canônico produz ANNOTATION. */
    private static JobDefinition annotationSourcedDefinition(String id) {
        return new JobDefinition(JobKey.of(id), null, Handler.class, new OnDemandSpec(),
                null, null, Misfire.IGNORE, true, 0, 0, null, null, DefinitionSource.ANNOTATION);
    }

    @Test
    void upsertInsertsANewCronDefinition() {
        JobDefinition definition = definition("welcome-email", new CronSpec("0 0 2 * * *", ZoneId.of("America/Sao_Paulo")));

        store.upsert(definition);
        Optional<StoredJob> stored = store.find(JobKey.of("welcome-email"));

        assertThat(stored).isPresent();
        assertThat(stored.get().definition()).isEqualTo(definition);
        assertThat(stored.get().orphaned()).isFalse();
        assertThat(stored.get().paused()).isFalse();
    }

    @Test
    void roundTripsIntervalSchedule() {
        JobDefinition definition = definition("poll", new IntervalSpec(Duration.ofMinutes(5), true));

        store.upsert(definition);

        assertThat(store.find(JobKey.of("poll")).map(StoredJob::definition)).contains(definition);
    }

    @Test
    void roundTripsOnDemandSchedule() {
        JobDefinition definition = definition("import-file", new OnDemandSpec());

        store.upsert(definition);

        assertThat(store.find(JobKey.of("import-file")).map(StoredJob::definition)).contains(definition);
    }

    @Test
    void upsertGeneratesAnIdDistinctPerJobKey() {
        store.upsert(definition("welcome-email", new OnDemandSpec()));
        store.upsert(definition("import-file", new OnDemandSpec()));

        JdbcTemplate rawJdbcTemplate = new JdbcTemplate(dataSource);
        String idA = rawJdbcTemplate.queryForObject("SELECT id FROM mohs_job_definitions WHERE job_key = ?", String.class, "welcome-email");
        String idB = rawJdbcTemplate.queryForObject("SELECT id FROM mohs_job_definitions WHERE job_key = ?", String.class, "import-file");

        assertThat(idA).isNotBlank();
        assertThat(idB).isNotBlank();
        assertThat(idA).isNotEqualTo(idB);
    }

    @Test
    void upsertKeepsTheSameIdAcrossReupsertOfTheSameJobKey() {
        store.upsert(definition("welcome-email", new OnDemandSpec()));
        JdbcTemplate rawJdbcTemplate = new JdbcTemplate(dataSource);
        String idBeforeReupsert = rawJdbcTemplate.queryForObject("SELECT id FROM mohs_job_definitions WHERE job_key = ?", String.class, "welcome-email");

        store.upsert(JobDefinition.of("welcome-email", Handler.class, spec -> spec.onDemand().retries(9)));

        String idAfterReupsert = rawJdbcTemplate.queryForObject("SELECT id FROM mohs_job_definitions WHERE job_key = ?", String.class, "welcome-email");
        assertThat(idAfterReupsert).isEqualTo(idBeforeReupsert);
    }

    @Test
    void upsertOnExistingKeyAppliesDefinitionalChanges() {
        store.upsert(definition("welcome-email", new CronSpec("0 0 2 * * *", ZoneId.of("UTC"))));

        JobDefinition changed = JobDefinition.of("welcome-email", Handler.class,
                spec -> spec.cron("0 0 3 * * *", ZoneId.of("UTC")).retries(9));
        store.upsert(changed);

        StoredJob stored = store.find(JobKey.of("welcome-email")).orElseThrow();
        assertThat(stored.definition().retries()).isEqualTo(9);
        assertThat(stored.definition().schedule()).isEqualTo(new CronSpec("0 0 3 * * *", ZoneId.of("UTC")));
    }

    @Test
    void upsertPreservesOperationalStateAcrossReupsert() {
        JobKey key = JobKey.of("welcome-email");
        store.upsert(definition("welcome-email", new OnDemandSpec()));
        store.pause(key);

        // redeploy: o código reaplica a mesma definição
        store.upsert(definition("welcome-email", new OnDemandSpec()));

        assertThat(store.find(key)).map(StoredJob::paused).contains(true);
    }

    @Test
    void findReturnsEmptyForUnknownKey() {
        assertThat(store.find(JobKey.of("ghost"))).isEmpty();
    }

    @Test
    void findAllReturnsEveryStoredJob() {
        store.upsert(definition("a", new OnDemandSpec()));
        store.upsert(definition("b", new OnDemandSpec()));

        try (Stream<StoredJob> all = store.findAll()) {
            assertThat(all).extracting(job -> job.definition().key()).containsExactlyInAnyOrder(JobKey.of("a"), JobKey.of("b"));
        }
    }

    /** Filtro por source aplicado no WHERE, não em memória — PROGRAMMATIC nunca sai do cursor. */
    @Test
    void findAllAnnotationSourcedExcludesProgrammaticJobs() {
        store.upsert(definition("programmatic-job", new OnDemandSpec()));
        store.upsert(annotationSourcedDefinition("annotation-job"));

        try (Stream<StoredJob> annotationSourced = store.findAllAnnotationSourced()) {
            assertThat(annotationSourced).extracting(job -> job.definition().key()).containsExactly(JobKey.of("annotation-job"));
        }
    }

    @Test
    void markOrphanedSetsTheFlag() {
        JobKey key = JobKey.of("welcome-email");
        store.upsert(definition("welcome-email", new OnDemandSpec()));

        store.markOrphaned(key);

        assertThat(store.find(key)).map(StoredJob::orphaned).contains(true);
    }

    /** orphaned é dedução do sistema, não decisão de operador como paused — reupsert (a anotação reapareceu) limpa. */
    @Test
    void upsertClearsOrphanedOnReupsert() {
        JobKey key = JobKey.of("welcome-email");
        store.upsert(definition("welcome-email", new OnDemandSpec()));
        store.markOrphaned(key);

        store.upsert(definition("welcome-email", new OnDemandSpec()));

        assertThat(store.find(key)).map(StoredJob::orphaned).contains(false);
    }

    @Test
    void pauseAndResumeToggleTheFlag() {
        JobKey key = JobKey.of("welcome-email");
        store.upsert(definition("welcome-email", new OnDemandSpec()));

        store.pause(key);
        assertThat(store.find(key)).map(StoredJob::paused).contains(true);

        store.resume(key);
        assertThat(store.find(key)).map(StoredJob::paused).contains(false);
    }

    /** mohs_executions.job_key tem FK pra mohs_job_definitions — o caso normal de um job aposentado é ter execuções. */
    private void seedExecution(String id, String jobKey, String state) {
        new JdbcTemplate(dataSource).update("""
                INSERT INTO mohs_executions (id, job_key, state, scheduled_at, actor, payload, payload_type, created_at)
                VALUES (?, ?, ?, ?, 'test', '{}', 'java.lang.String', ?)
                """, id, jobKey, state,
                JdbcTimestamps.toUtcTimestamp(clock.instant()), JdbcTimestamps.toUtcTimestamp(clock.instant()));
    }

    /**
     * Soft-retire ({@code Mohs.remove}: "cancela fires futuros, preserva
     * histórico"): a definição some das leituras, mas a linha fica — a FK
     * de {@code mohs_executions} nunca derruba a chamada — e as execuções
     * {@code ENQUEUED} são canceladas, nunca deixadas na fila.
     */
    @Test
    void removeRetiresTheJobCancellingEnqueuedAndPreservingHistory() {
        JobKey key = JobKey.of("welcome-email");
        store.upsert(definition("welcome-email", new OnDemandSpec()));
        seedExecution("exec-done", "welcome-email", "SUCCEEDED");
        seedExecution("exec-queued", "welcome-email", "ENQUEUED");

        store.remove(key);

        assertThat(store.find(key)).isEmpty();
        try (Stream<StoredJob> all = store.findAll()) {
            assertThat(all).isEmpty();
        }
        JdbcTemplate raw = new JdbcTemplate(dataSource);
        assertThat(raw.queryForObject("SELECT state FROM mohs_executions WHERE id = ?", String.class, "exec-done")).isEqualTo("SUCCEEDED");
        assertThat(raw.queryForObject("SELECT state FROM mohs_executions WHERE id = ?", String.class, "exec-queued")).isEqualTo("CANCELLED");
    }

    /** ADR-0033: RETRY_SCHEDULED também é claimável — fora do cancel do remove, ficaria presa pra sempre (claim filtra retired, reaper só vê RUNNING). */
    @Test
    void removeCancelsRetryScheduledExecutionsToo() {
        JobKey key = JobKey.of("welcome-email");
        store.upsert(definition("welcome-email", new OnDemandSpec()));
        seedExecution("exec-retry", "welcome-email", "RETRY_SCHEDULED");

        store.remove(key);

        JdbcTemplate raw = new JdbcTemplate(dataSource);
        assertThat(raw.queryForObject("SELECT state FROM mohs_executions WHERE id = ?", String.class, "exec-retry")).isEqualTo("CANCELLED");
    }

    /** Mesmo racional de {@link #upsertClearsOrphanedOnReupsert}: o upsert acontecer prova que uma fonte real quer o job de novo. */
    @Test
    void upsertAfterRemoveResurrectsTheDefinitionWithItsHistory() {
        JobKey key = JobKey.of("welcome-email");
        store.upsert(definition("welcome-email", new OnDemandSpec()));
        seedExecution("exec-done", "welcome-email", "SUCCEEDED");
        store.remove(key);

        store.upsert(definition("welcome-email", new OnDemandSpec()));

        assertThat(store.find(key)).isPresent();
        JdbcTemplate raw = new JdbcTemplate(dataSource);
        assertThat(raw.queryForObject("SELECT COUNT(*) FROM mohs_executions WHERE job_key = ?", Integer.class, "welcome-email")).isEqualTo(1);
    }

    /** CONC-2 — dois nós vendo 0 linhas no UPDATE e disputando o INSERT de primeira vez. */
    @Test
    void upsertHandlesConcurrentFirstTimeInsertWithoutThrowing() throws Exception {
        JobDefinition definitionToRegister = definition("welcome-email", new OnDemandSpec());
        CyclicBarrier barrier = new CyclicBarrier(2);
        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        Callable<JobDefinition> upsert = () -> {
            barrier.await();
            return store.upsert(definitionToRegister);
        };

        Future<JobDefinition> futureA = executor.submit(upsert);
        Future<JobDefinition> futureB = executor.submit(upsert);
        futureA.get(10, TimeUnit.SECONDS);
        futureB.get(10, TimeUnit.SECONDS);
        executor.shutdown();
        assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();

        assertThat(store.find(JobKey.of("welcome-email"))).map(StoredJob::definition).contains(definitionToRegister);
    }

    @Test
    void upsertRoundTripsMaxConcurrentExecutionsAndStartsWithZeroRunning() {
        store.upsert(definitionWithCap("report-summary", 10));

        StoredJob stored = store.find(JobKey.of("report-summary")).orElseThrow();

        assertThat(stored.definition().maxConcurrentExecutions()).isEqualTo(10);
        assertThat(stored.runningExecutionCount()).isZero();
    }

    @Test
    void tryIncrementRunningExecutionsReservesASlotWhenBelowLimit() {
        store.upsert(definitionWithCap("report-summary", 2));
        JobKey key = JobKey.of("report-summary");

        assertThat(store.tryIncrementRunningExecutions(key)).isTrue();

        assertThat(store.find(key)).map(StoredJob::runningExecutionCount).contains(1);
    }

    @Test
    void tryIncrementRunningExecutionsFailsWhenAtLimit() {
        store.upsert(definitionWithCap("report-summary", 1));
        JobKey key = JobKey.of("report-summary");
        assertThat(store.tryIncrementRunningExecutions(key)).isTrue();

        assertThat(store.tryIncrementRunningExecutions(key)).isFalse();

        assertThat(store.find(key)).map(StoredJob::runningExecutionCount).contains(1);
    }

    @Test
    void tryIncrementRunningExecutionsIsAtomicUnderConcurrentContention() throws InterruptedException {
        store.upsert(definitionWithCap("report-summary", 10));
        JobKey key = JobKey.of("report-summary");
        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        AtomicInteger accepted = new AtomicInteger();

        IntStream.range(0, 100).forEach(i -> executor.submit(() -> {
            if (store.tryIncrementRunningExecutions(key)) {
                accepted.incrementAndGet();
            }
        }));
        executor.shutdown();
        assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();

        assertThat(accepted.get()).isEqualTo(10);
        assertThat(store.find(key)).map(StoredJob::runningExecutionCount).contains(10);
    }

    @Test
    void decrementRunningExecutionsReleasesAReservedSlot() {
        store.upsert(definitionWithCap("report-summary", 2));
        JobKey key = JobKey.of("report-summary");
        store.tryIncrementRunningExecutions(key);

        store.decrementRunningExecutions(key);

        assertThat(store.find(key)).map(StoredJob::runningExecutionCount).contains(0);
    }

    @Test
    void decrementRunningExecutionsNeverGoesBelowZero() {
        store.upsert(definitionWithCap("report-summary", 2));
        JobKey key = JobKey.of("report-summary");

        store.decrementRunningExecutions(key);

        assertThat(store.find(key)).map(StoredJob::runningExecutionCount).contains(0);
    }

    @Test
    void findSkipsRowsWhoseHandlerTypeNoLongerResolves() {
        // simula um handler removido do código: insere a linha direto,
        // sem passar pela store (que exige um Class<?> de verdade).
        Timestamp now = Timestamp.from(clock.instant());
        JdbcTemplate rawJdbcTemplate = new JdbcTemplate(dataSource);
        rawJdbcTemplate.update("""
                INSERT INTO mohs_job_definitions (
                    id, job_key, handler_type, schedule_type, misfire, retries, source, orphaned, paused, created_at, updated_at)
                VALUES ('ghost-id', 'ghost-handler', 'com.example.LongGoneHandler', 'ON_DEMAND', 'IGNORE', 0, 'ANNOTATION', TRUE, FALSE, ?, ?)
                """, now, now);
        store.upsert(definition("still-here", new OnDemandSpec()));

        try (Stream<StoredJob> all = store.findAll()) {
            assertThat(all).extracting(job -> job.definition().key()).containsExactly(JobKey.of("still-here"));
        }
    }

    /** JAVA-4: schedule_type corrompido não pode virar ON_DEMAND em silêncio — mesmo espírito do DUP-3, um valor de lixo tem que ser barulhento, não uma variante válida por acidente. */
    @Test
    void findFailsFastOnUnknownScheduleType() {
        Timestamp now = Timestamp.from(clock.instant());
        JdbcTemplate rawJdbcTemplate = new JdbcTemplate(dataSource);
        rawJdbcTemplate.update("""
                INSERT INTO mohs_job_definitions (
                    id, job_key, handler_type, schedule_type, misfire, retries, source, orphaned, paused, created_at, updated_at)
                VALUES ('corrupt-id', 'corrupt-schedule', ?, 'GARBAGE', 'IGNORE', 0, 'ANNOTATION', FALSE, FALSE, ?, ?)
                """, Handler.class.getName(), now, now);

        assertThatThrownBy(() -> store.find(JobKey.of("corrupt-schedule")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("GARBAGE")
                .hasMessageContaining("corrupt-schedule");
    }

    /** DUP-3: uma linha com handler não resolvido se torna visível como ORPHANED em vez de simplesmente sumir dos dois lados (find/findAll). */
    @Test
    void findAllMarksRowsWithUnresolvedHandlerAsOrphaned() {
        Timestamp now = Timestamp.from(clock.instant());
        JdbcTemplate rawJdbcTemplate = new JdbcTemplate(dataSource);
        rawJdbcTemplate.update("""
                INSERT INTO mohs_job_definitions (
                    id, job_key, handler_type, schedule_type, misfire, retries, source, orphaned, paused, created_at, updated_at)
                VALUES ('ghost-id', 'ghost-handler', 'com.example.LongGoneHandler', 'ON_DEMAND', 'IGNORE', 0, 'ANNOTATION', FALSE, FALSE, ?, ?)
                """, now, now);

        try (Stream<StoredJob> all = store.findAll()) {
            all.forEach(job -> { });
        }

        Boolean orphaned = rawJdbcTemplate.queryForObject(
                "SELECT orphaned FROM mohs_job_definitions WHERE job_key = ?", Boolean.class, "ghost-handler");
        assertThat(orphaned).isTrue();
    }

    @Test
    void findMarksTheRowWithUnresolvedHandlerAsOrphaned() {
        Timestamp now = Timestamp.from(clock.instant());
        JdbcTemplate rawJdbcTemplate = new JdbcTemplate(dataSource);
        rawJdbcTemplate.update("""
                INSERT INTO mohs_job_definitions (
                    id, job_key, handler_type, schedule_type, misfire, retries, source, orphaned, paused, created_at, updated_at)
                VALUES ('ghost-id', 'ghost-handler', 'com.example.LongGoneHandler', 'ON_DEMAND', 'IGNORE', 0, 'ANNOTATION', FALSE, FALSE, ?, ?)
                """, now, now);

        assertThat(store.find(JobKey.of("ghost-handler"))).isEmpty();

        Boolean orphaned = rawJdbcTemplate.queryForObject(
                "SELECT orphaned FROM mohs_job_definitions WHERE job_key = ?", Boolean.class, "ghost-handler");
        assertThat(orphaned).isTrue();
    }
}
