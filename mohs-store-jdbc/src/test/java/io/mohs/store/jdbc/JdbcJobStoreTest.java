package io.mohs.store.jdbc;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
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
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
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

    /** ADR-0037: startPaused arma só o nascimento — o job nasce pausado e o trigger não é varrido até o resume. */
    @Test
    void upsertCreatesAStartPausedJobBornPaused() {
        store.upsert(JobDefinition.of("dormant", Handler.class, spec -> spec.every(Duration.ofMinutes(1)).startPaused()));

        assertThat(store.find(JobKey.of("dormant")).orElseThrow().paused()).isTrue();
        clock.advance(Duration.ofMinutes(5));
        assertThat(store.findDueRecurring(clock.instant(), 10)).isEmpty();
    }

    /** Depois do nascimento, paused é do operador (ADR-0006) — redeploy com startPaused=true nunca re-pausa um job que o operador ligou. */
    @Test
    void redeployNeverReappliesStartPaused() {
        JobDefinition definition = JobDefinition.of("dormant", Handler.class, spec -> spec.every(Duration.ofMinutes(1)).startPaused());
        store.upsert(definition);
        store.resume(JobKey.of("dormant"));

        store.upsert(definition);

        assertThat(store.find(JobKey.of("dormant")).orElseThrow().paused()).isFalse();
    }

    /** O happy path completo da feature: nasce pausado → resume → o trigger armado no nascimento (agora envelhecido) fica devido — a política de misfire decide dali (ADR-0035). */
    @Test
    void resumedDormantJobBecomesDueWithItsBirthTrigger() {
        store.upsert(JobDefinition.of("dormant", Handler.class, spec -> spec.every(Duration.ofMinutes(1)).startPaused()));
        clock.advance(Duration.ofMinutes(5));
        assertThat(store.findDueRecurring(clock.instant(), 10)).isEmpty();

        store.resume(JobKey.of("dormant"));

        assertThat(store.findDueRecurring(clock.instant(), 10))
                .extracting(stored -> stored.definition().key().value())
                .containsExactly("dormant");
    }

    /** "A linha é a memória" (review ADR-0037): soft-retire preserva a linha, então ressurreição via upsert NÃO re-aplica startPaused — paused volta como o operador o deixou. */
    @Test
    void resurrectionAfterRetireKeepsTheOperatorPauseState() {
        JobDefinition definition = JobDefinition.of("dormant", Handler.class, spec -> spec.every(Duration.ofMinutes(1)).startPaused());
        store.upsert(definition);
        store.resume(JobKey.of("dormant"));
        store.remove(JobKey.of("dormant"));

        store.upsert(definition);

        assertThat(store.find(JobKey.of("dormant")).orElseThrow().paused()).isFalse();
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
                JdbcTimestamps.toUtcLocalDateTime(clock.instant()), JdbcTimestamps.toUtcLocalDateTime(clock.instant()));
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

    /** ADR-0033: RETRY_WAITING também é claimável — fora do cancel do remove, ficaria presa pra sempre (claim filtra retired, reaper só vê RUNNING). */
    @Test
    void removeCancelsRetryScheduledExecutionsToo() {
        JobKey key = JobKey.of("welcome-email");
        store.upsert(definition("welcome-email", new OnDemandSpec()));
        seedExecution("exec-retry", "welcome-email", "RETRY_WAITING");

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

    /** ADR-0047: o bloco devolve N vagas numa escrita, mesmo estado final de N decrementos guardados — inclusive o piso. */
    @Test
    void bulkDecrementReleasesSeveralSlotsAndFloorsAtZero() {
        store.upsert(definitionWithCap("report-summary", 10));
        JobKey key = JobKey.of("report-summary");
        for (int i = 0; i < 3; i++) {
            store.tryIncrementRunningExecutions(key);
        }

        store.decrementRunningExecutions(key, 2);
        assertThat(store.find(key)).map(StoredJob::runningExecutionCount).contains(1);

        store.decrementRunningExecutions(key, 5);
        assertThat(store.find(key)).map(StoredJob::runningExecutionCount).contains(0);
    }

    @Test
    void findSkipsRowsWhoseHandlerTypeNoLongerResolves() {
        // simula um handler removido do código: insere a linha direto,
        // sem passar pela store (que exige um Class<?> de verdade).
        LocalDateTime now = JdbcTimestamps.toUtcLocalDateTime(clock.instant());
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
        LocalDateTime now = JdbcTimestamps.toUtcLocalDateTime(clock.instant());
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
        LocalDateTime now = JdbcTimestamps.toUtcLocalDateTime(clock.instant());
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
        LocalDateTime now = JdbcTimestamps.toUtcLocalDateTime(clock.instant());
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

    /** O estado do trigger (ADR-0035): quem arma/preserva/recalcula/cura o {@code next_fire_at} é o upsert; a leitura devida é {@code findDueRecurring}. */
    @Nested
    class TriggerState {

        private JdbcTemplate rawJdbc() {
            return new JdbcTemplate(dataSource);
        }

        private @Nullable Instant nextFireAtOf(String jobKey) {
            LocalDateTime stored = rawJdbc().queryForObject(
                    "SELECT next_fire_at FROM mohs_job_definitions WHERE job_key = ?", LocalDateTime.class, jobKey);
            return stored == null ? null : JdbcTimestamps.fromUtcLocalDateTime(stored);
        }

        private void disarm(String jobKey) {
            rawJdbc().update("UPDATE mohs_job_definitions SET next_fire_at = NULL WHERE job_key = ?", jobKey);
        }

        @Test
        void upsertArmsTheTriggerOfACronDefinition() {
            store.upsert(definition("welcome-email", new CronSpec("0 0 2 * * *", ZoneId.of("America/Sao_Paulo"))));

            // base 2026-08-13T00:00Z = 12/08 21:00 em São Paulo → próximas 02:00 SP = 05:00Z de 13/08
            assertThat(nextFireAtOf("welcome-email")).isEqualTo(Instant.parse("2026-08-13T05:00:00Z"));
        }

        @Test
        void upsertArmsTheTriggerOfAFixedRateDefinition() {
            store.upsert(definition("poll", new IntervalSpec(Duration.ofMinutes(5), false)));

            assertThat(nextFireAtOf("poll")).isEqualTo(clock.instant().plus(Duration.ofMinutes(5)));
        }

        @Test
        void upsertLeavesOnDemandUnarmed() {
            store.upsert(definition("import-file", new OnDemandSpec()));

            assertThat(nextFireAtOf("import-file")).isNull();
        }

        /** Sem isto, todo redeploy de um job every-30min empurraria o disparo para sempre. */
        @Test
        void upsertPreservesTheTriggerWhenTheScheduleIsUnchanged() {
            JobDefinition definition = definition("poll", new IntervalSpec(Duration.ofMinutes(5), false));
            store.upsert(definition);
            Instant armedAt = nextFireAtOf("poll");

            clock.advance(Duration.ofMinutes(10));
            store.upsert(definition);

            assertThat(nextFireAtOf("poll")).isEqualTo(armedAt);
        }

        @Test
        void upsertRecomputesTheTriggerWhenTheScheduleChanges() {
            store.upsert(definition("poll", new IntervalSpec(Duration.ofMinutes(5), false)));

            clock.advance(Duration.ofMinutes(10));
            store.upsert(definition("poll", new IntervalSpec(Duration.ofMinutes(1), false)));

            assertThat(nextFireAtOf("poll")).isEqualTo(clock.instant().plus(Duration.ofMinutes(1)));
        }

        /** Cura de coluna nova em base velha (pré-ADR-0035): agenda recorrente com next_fire_at NULL rearma no boot. */
        @Test
        void upsertHealsAnUnarmedRecurringTrigger() {
            JobDefinition definition = definition("poll", new IntervalSpec(Duration.ofMinutes(5), false));
            store.upsert(definition);
            disarm("poll");

            store.upsert(definition);

            assertThat(nextFireAtOf("poll")).isEqualTo(clock.instant().plus(Duration.ofMinutes(5)));
        }

        /** afterFinish com ocorrência viva do scheduler NÃO cura — armar criaria a sobreposição que fixed-delay promete não ter. */
        @Test
        void upsertDoesNotHealAnAfterFinishChainWithALiveSchedulerOccurrence() {
            JobDefinition definition = definition("poll", new IntervalSpec(Duration.ofMinutes(5), true));
            store.upsert(definition);
            disarm("poll");
            seedExecution("occ-1", "poll", "scheduler", "RUNNING");

            store.upsert(definition);

            assertThat(nextFireAtOf("poll")).isNull();
        }

        @Test
        void upsertHealsAnAfterFinishChainWhoseOccurrencesAllFinished() {
            JobDefinition definition = definition("poll", new IntervalSpec(Duration.ofMinutes(5), true));
            store.upsert(definition);
            disarm("poll");
            seedExecution("occ-1", "poll", "scheduler", "SUCCEEDED");
            seedExecution("man-1", "poll", "api:user", "RUNNING"); // execução manual não é a corrente

            store.upsert(definition);

            assertThat(nextFireAtOf("poll")).isEqualTo(clock.instant().plus(Duration.ofMinutes(5)));
        }

        @Test
        void findDueRecurringReturnsDueJobsOldestFirstWithinTheLimit() {
            store.upsert(definition("due-late", new IntervalSpec(Duration.ofMinutes(1), false)));
            clock.advance(Duration.ofMinutes(2));
            store.upsert(definition("due-recent", new IntervalSpec(Duration.ofMinutes(1), false)));
            store.upsert(definition("not-due", new IntervalSpec(Duration.ofHours(6), false)));
            store.upsert(definition("import-file", new OnDemandSpec()));
            store.upsert(definition("paused", new IntervalSpec(Duration.ofMinutes(1), false)));
            store.pause(JobKey.of("paused"));
            store.upsert(definition("orphaned", new IntervalSpec(Duration.ofMinutes(1), false)));
            store.markOrphaned(JobKey.of("orphaned"));
            clock.advance(Duration.ofMinutes(2));

            assertThat(store.findDueRecurring(clock.instant(), 10))
                    .extracting(stored -> stored.definition().key().value())
                    .containsExactly("due-late", "due-recent");
            assertThat(store.findDueRecurring(clock.instant(), 1))
                    .extracting(stored -> stored.definition().key().value())
                    .containsExactly("due-late");
        }

        @Test
        void findDueRecurringExposesTheStoredNextFireAt() {
            store.upsert(definition("poll", new IntervalSpec(Duration.ofMinutes(1), false)));
            Instant armedAt = clock.instant().plus(Duration.ofMinutes(1));
            clock.advance(Duration.ofMinutes(2));

            assertThat(store.findDueRecurring(clock.instant(), 10))
                    .singleElement()
                    .extracting(StoredJob::nextFireAt)
                    .isEqualTo(armedAt);
        }

        /**
         * Review ADR-0035 (lost update): preservar é NÃO escrever a coluna.
         * A corrida real (CAS de disparo/rearme entre o snapshot e o UPDATE
         * do upsert) não é reproduzível em teste unitário — o que se pina é
         * o mecanismo que a elimina: com agenda inalterada, {@code
         * next_fire_at} fica FORA do statement, então não existe escrita
         * capaz de regredir um avanço concorrente.
         */
        @Test
        void upsertOmitsTheTriggerColumnWhenPreserving() {
            JobDefinition definition = definition("poll", new IntervalSpec(Duration.ofMinutes(5), false));
            store.upsert(definition);
            List<String> statements = new ArrayList<>();
            JdbcJobStore spying = new JdbcJobStore(sqlRecordingDataSource(statements), clock);

            spying.upsert(definition); // agenda inalterada — preserva

            assertThat(statements)
                    .filteredOn(sql -> sql.contains("UPDATE mohs_job_definitions"))
                    .isNotEmpty()
                    .noneMatch(sql -> sql.contains("next_fire_at"));
        }

        /** O contraponto: mudança de agenda escreve a coluna — reconfiguração explícita vence disparo concorrente (ADR-0035). */
        @Test
        void upsertWritesTheTriggerColumnWhenTheScheduleChanges() {
            store.upsert(definition("poll", new IntervalSpec(Duration.ofMinutes(5), false)));
            List<String> statements = new ArrayList<>();
            JdbcJobStore spying = new JdbcJobStore(sqlRecordingDataSource(statements), clock);

            spying.upsert(definition("poll", new IntervalSpec(Duration.ofMinutes(1), false)));

            // o template já substituiu os parâmetros nomeados por ? quando o SQL chega no prepareStatement
            assertThat(statements)
                    .filteredOn(sql -> sql.contains("UPDATE mohs_job_definitions"))
                    .anyMatch(sql -> sql.contains("next_fire_at = ?"));
        }

        /** DataSource que grava o SQL de todo prepareStatement — pass-through no resto (mesma técnica de proxy de JdbcExecutionStoreTest). */
        private DataSource sqlRecordingDataSource(List<String> statements) {
            InvocationHandler onDataSource = (_, method, args) -> {
                Object result = method.invoke(dataSource, args);
                if (result instanceof Connection connection) {
                    return Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{Connection.class},
                            (_, connectionMethod, connectionArgs) -> {
                                if (connectionMethod.getName().equals("prepareStatement")
                                        && connectionArgs != null && connectionArgs.length > 0
                                        && connectionArgs[0] instanceof String sql) {
                                    statements.add(sql);
                                }
                                return connectionMethod.invoke(connection, connectionArgs);
                            });
                }
                return result;
            };
            return (DataSource) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{DataSource.class}, onDataSource);
        }

        /** ADR-0036: agenda nova e trigger recomputado aterrissam no MESMO UPDATE; políticas (retries etc.) intactas. */
        @Test
        void rescheduleSwapsTheScheduleAndRearmsTheTrigger() {
            store.upsert(definition("poll", new IntervalSpec(Duration.ofMinutes(5), false)));

            boolean rescheduled = store.reschedule(JobKey.of("poll"), new IntervalSpec(Duration.ofMinutes(1), false));

            assertThat(rescheduled).isTrue();
            StoredJob stored = store.find(JobKey.of("poll")).orElseThrow();
            assertThat(stored.definition().schedule()).isEqualTo(new IntervalSpec(Duration.ofMinutes(1), false));
            assertThat(stored.definition().retries()).isEqualTo(3); // política não é agenda — intacta
            assertThat(stored.nextFireAt()).isEqualTo(clock.instant().plus(Duration.ofMinutes(1)));
        }

        /** Desligar a recorrência em runtime é um caso válido do mesmo endpoint (ADR-0036). */
        @Test
        void rescheduleToOnDemandDisarmsTheTrigger() {
            store.upsert(definition("poll", new IntervalSpec(Duration.ofMinutes(5), false)));

            store.reschedule(JobKey.of("poll"), new OnDemandSpec());

            assertThat(store.find(JobKey.of("poll")).orElseThrow().definition().schedule()).isEqualTo(new OnDemandSpec());
            assertThat(nextFireAtOf("poll")).isNull();
        }

        @Test
        void rescheduleOfARetiredOrUnknownJobReturnsFalse() {
            assertThat(store.reschedule(JobKey.of("ghost"), new OnDemandSpec())).isFalse();

            store.upsert(definition("poll", new IntervalSpec(Duration.ofMinutes(5), false)));
            store.remove(JobKey.of("poll"));
            assertThat(store.reschedule(JobKey.of("poll"), new OnDemandSpec())).isFalse();
        }

        /** ADR-0036 × ADR-0035: corrente fixed-delay em voo (trigger desarmado) reagendada — a série nova vence; o rearme atrasado da conclusão no-opa no guard IS NULL. */
        @Test
        void rescheduleArmsTheNewSeriesAndTheLateCompletionRearmLoses() {
            store.upsert(definition("chain", new IntervalSpec(Duration.ofMinutes(5), true)));
            disarm("chain"); // ocorrência em voo: next_fire_at = NULL, como o fire deixa

            store.reschedule(JobKey.of("chain"), new IntervalSpec(Duration.ofMinutes(1), true));
            store.armNextFire(JobKey.of("chain"), clock.instant().plus(Duration.ofMinutes(5))); // conclusão chega atrasada

            assertThat(nextFireAtOf("chain")).isEqualTo(clock.instant().plus(Duration.ofMinutes(1)));
        }

        /** Cron irrealizável falha ANTES de qualquer escrita — a agenda antiga fica intacta. */
        @Test
        void rescheduleWithAnImpossibleCronFailsWithoutWriting() {
            store.upsert(definition("poll", new IntervalSpec(Duration.ofMinutes(5), false)));

            assertThatThrownBy(() -> store.reschedule(JobKey.of("poll"), new CronSpec("0 0 0 30 2 *", ZoneId.of("UTC"))))
                    .isInstanceOf(IllegalArgumentException.class);

            assertThat(store.find(JobKey.of("poll")).orElseThrow().definition().schedule())
                    .isEqualTo(new IntervalSpec(Duration.ofMinutes(5), false));
        }

        @Test
        void armNextFireOnlyArmsAnUnarmedTrigger() {
            store.upsert(definition("poll", new IntervalSpec(Duration.ofMinutes(5), true)));
            Instant armedAt = nextFireAtOf("poll");

            store.armNextFire(JobKey.of("poll"), clock.instant().plus(Duration.ofHours(1)));
            assertThat(nextFireAtOf("poll")).isEqualTo(armedAt); // já armado — guard IS NULL protege

            disarm("poll");
            Instant rearmAt = clock.instant().plus(Duration.ofHours(1));
            store.armNextFire(JobKey.of("poll"), rearmAt);
            assertThat(nextFireAtOf("poll")).isEqualTo(rearmAt);
        }

        private void seedExecution(String id, String jobKey, String actor, String state) {
            LocalDateTime now = JdbcTimestamps.toUtcLocalDateTime(clock.instant());
            rawJdbc().update("""
                    INSERT INTO mohs_executions (id, job_key, state, scheduled_at, actor, priority, payload, payload_type, created_at)
                    VALUES (?, ?, ?, ?, ?, 20, '{}', 'java.util.LinkedHashMap', ?)
                    """, id, jobKey, state, now, actor, now);
        }
    }

    /**
     * Aposentar o job cancela os pendentes, e cancelar é terminal: sem contar,
     * o membro some do lote, pending nunca zera e o lote fica aberto para
     * sempre — não há reconciliação que cure (ADR-0043). Em bloco por lote,
     * não uma chamada por membro.
     */
    @Test
    void removeCountsCancelledBatchMembersIntoTheirBatch() {
        JdbcTemplate raw = new JdbcTemplate(dataSource);
        store.upsert(definition("welcome-email", new OnDemandSpec()));
        new JdbcBatchStore(dataSource, clock).insert("b6", 3);
        seedExecution("m1", "welcome-email", "ENQUEUED");
        seedExecution("m2", "welcome-email", "RETRY_WAITING");
        seedExecution("m3", "welcome-email", "SUCCEEDED");
        raw.update("UPDATE mohs_executions SET batch_id = ? WHERE id IN (?, ?, ?)", "b6", "m1", "m2", "m3");

        store.remove(JobKey.of("welcome-email"));

        // só os dois pendentes foram cancelados agora; o SUCCEEDED já era terminal
        assertThat(raw.queryForObject("SELECT failed FROM mohs_batches WHERE id = ?", Integer.class, "b6"))
                .isEqualTo(2);
    }
}
