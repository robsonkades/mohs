package io.mohs.test;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

import io.mohs.core.definition.DefinitionSource;
import io.mohs.core.definition.JobDefinition;
import io.mohs.core.job.JobKey;
import io.mohs.core.schedule.CronSpec;
import io.mohs.core.schedule.Misfire;
import io.mohs.core.schedule.OnDemandSpec;
import io.mohs.engine.StoredJob;

import static org.assertj.core.api.Assertions.assertThat;

class InMemoryJobStoreTest {

    record Handler() {
    }

    private static JobDefinition definition(String id) {
        return JobDefinition.of(id, Handler.class, spec -> spec.onDemand().runner("io"));
    }

    private static JobDefinition definitionWithCap(String id, int max) {
        return JobDefinition.of(id, Handler.class, spec -> spec.onDemand().maxConcurrentExecutions(max));
    }

    /** JobDefinition.of (builder público) hardcoda PROGRAMMATIC — só o construtor canônico produz ANNOTATION. */
    private static JobDefinition annotationSourcedDefinition(String id) {
        return new JobDefinition(JobKey.of(id), null, Handler.class, new OnDemandSpec(),
                null, null, Misfire.IGNORE, true, 0, 0, null, null, DefinitionSource.ANNOTATION);
    }

    private final InMemoryJobStore store = new InMemoryJobStore();

    @Test
    void upsertInsertsANewDefinition() {
        JobDefinition definition = definition("welcome-email");

        store.upsert(definition);
        Optional<StoredJob> stored = store.find(JobKey.of("welcome-email"));

        assertThat(stored).isPresent();
        assertThat(stored.get().definition()).isEqualTo(definition);
        assertThat(stored.get().orphaned()).isFalse();
        assertThat(stored.get().paused()).isFalse();
    }

    /** ADR-0035 no test kit: upsert arma o trigger com o relógio determinístico; agenda inalterada preserva, alterada recalcula. */
    @Test
    void upsertArmsPreservesAndRecomputesTheTrigger() {
        MutableClock clock = MutableClock.startingAt(Instant.parse("2026-08-15T12:00:00Z"));
        InMemoryJobStore clocked = new InMemoryJobStore(clock);
        JobDefinition every5m = JobDefinition.of("poll", Handler.class, spec -> spec.every(Duration.ofMinutes(5)));

        clocked.upsert(every5m);
        Instant armedAt = clocked.find(JobKey.of("poll")).orElseThrow().nextFireAt();
        assertThat(armedAt).isEqualTo(Instant.parse("2026-08-15T12:05:00Z"));

        clock.advance(Duration.ofMinutes(10));
        clocked.upsert(every5m);
        assertThat(clocked.find(JobKey.of("poll")).orElseThrow().nextFireAt()).isEqualTo(armedAt);

        clocked.upsert(JobDefinition.of("poll", Handler.class, spec -> spec.every(Duration.ofMinutes(1))));
        assertThat(clocked.find(JobKey.of("poll")).orElseThrow().nextFireAt())
                .isEqualTo(clock.instant().plus(Duration.ofMinutes(1)));
    }

    /** ADR-0037 no test kit: nasce pausado no primeiro registro; depois disso, paused é do operador — redeploy nunca re-pausa. */
    @Test
    void startPausedAppliesOnlyAtBirth() {
        JobDefinition dormant = JobDefinition.of("dormant", Handler.class, spec -> spec.every(Duration.ofMinutes(1)).startPaused());
        store.upsert(dormant);
        assertThat(store.find(JobKey.of("dormant")).orElseThrow().paused()).isTrue();

        store.resume(JobKey.of("dormant"));
        store.upsert(dormant);
        assertThat(store.find(JobKey.of("dormant")).orElseThrow().paused()).isFalse();
    }

    @Test
    void findDueRecurringReturnsDueUnpausedJobsOldestFirstWithinTheLimit() {
        MutableClock clock = MutableClock.startingAt(Instant.parse("2026-08-15T12:00:00Z"));
        InMemoryJobStore clocked = new InMemoryJobStore(clock);
        clocked.upsert(JobDefinition.of("due-late", Handler.class, spec -> spec.every(Duration.ofMinutes(1))));
        clock.advance(Duration.ofMinutes(2));
        clocked.upsert(JobDefinition.of("due-recent", Handler.class, spec -> spec.every(Duration.ofMinutes(1))));
        clocked.upsert(JobDefinition.of("not-due", Handler.class, spec -> spec.every(Duration.ofHours(6))));
        clocked.upsert(JobDefinition.of("import-file", Handler.class, spec -> spec.onDemand()));
        clocked.upsert(JobDefinition.of("paused", Handler.class, spec -> spec.every(Duration.ofMinutes(1))));
        clocked.pause(JobKey.of("paused"));
        clock.advance(Duration.ofMinutes(2));

        assertThat(clocked.findDueRecurring(clock.instant(), 10))
                .extracting(stored -> stored.definition().key().value())
                .containsExactly("due-late", "due-recent");
        assertThat(clocked.findDueRecurring(clock.instant(), 1))
                .extracting(stored -> stored.definition().key().value())
                .containsExactly("due-late");
    }

    /** ADR-0036 no test kit: mesma semântica do adapter JDBC — agenda trocada, trigger recomputado, políticas e estado operacional intactos. */
    @Test
    void rescheduleSwapsTheScheduleAndRearmsTheTrigger() {
        MutableClock clock = MutableClock.startingAt(Instant.parse("2026-08-15T12:00:00Z"));
        InMemoryJobStore clocked = new InMemoryJobStore(clock);
        clocked.upsert(JobDefinition.of("poll", Handler.class, spec -> spec.every(Duration.ofMinutes(5)).retries(3)));
        clocked.pause(JobKey.of("poll"));

        boolean rescheduled = clocked.reschedule(JobKey.of("poll"), new CronSpec("0 0 2 * * *", ZoneId.of("UTC")));

        assertThat(rescheduled).isTrue();
        StoredJob stored = clocked.find(JobKey.of("poll")).orElseThrow();
        assertThat(stored.definition().schedule()).isEqualTo(new CronSpec("0 0 2 * * *", ZoneId.of("UTC")));
        assertThat(stored.definition().retries()).isEqualTo(3);
        assertThat(stored.paused()).isTrue(); // estado operacional não é agenda
        assertThat(stored.nextFireAt()).isEqualTo(Instant.parse("2026-08-16T02:00:00Z"));
        assertThat(clocked.reschedule(JobKey.of("ghost"), new OnDemandSpec())).isFalse();
    }

    @Test
    void armNextFireOnlyArmsAnUnarmedTrigger() {
        store.upsert(definition("import-file")); // on-demand: nasce desarmado
        Instant armAt = Instant.parse("2026-08-15T12:00:00Z");

        store.armNextFire(JobKey.of("import-file"), armAt);
        assertThat(store.find(JobKey.of("import-file")).orElseThrow().nextFireAt()).isEqualTo(armAt);

        store.armNextFire(JobKey.of("import-file"), armAt.plusSeconds(60));
        assertThat(store.find(JobKey.of("import-file")).orElseThrow().nextFireAt()).isEqualTo(armAt);
    }

    @Test
    void upsertOnExistingKeyAppliesDefinitionalChanges() {
        store.upsert(definition("welcome-email"));

        JobDefinition changed = JobDefinition.of("welcome-email", Handler.class,
                spec -> spec.cron("0 0 2 * * *", ZoneId.of("UTC")).retries(5));
        store.upsert(changed);

        StoredJob stored = store.find(JobKey.of("welcome-email")).orElseThrow();
        assertThat(stored.definition().retries()).isEqualTo(5);
        assertThat(stored.definition().schedule()).isEqualTo(new CronSpec("0 0 2 * * *", ZoneId.of("UTC")));
    }

    @Test
    void upsertPreservesOperationalStateAcrossReupsert() {
        JobKey key = JobKey.of("welcome-email");
        store.upsert(definition("welcome-email"));
        store.pause(key);

        store.upsert(definition("welcome-email"));

        assertThat(store.find(key)).map(StoredJob::paused).contains(true);
    }

    @Test
    void findReturnsEmptyForUnknownKey() {
        assertThat(store.find(JobKey.of("ghost"))).isEmpty();
    }

    @Test
    void findAllReturnsEveryStoredJob() {
        store.upsert(definition("a"));
        store.upsert(definition("b"));

        try (Stream<StoredJob> all = store.findAll()) {
            List<JobKey> keys = all.map(job -> job.definition().key()).toList();
            assertThat(keys).containsExactlyInAnyOrder(JobKey.of("a"), JobKey.of("b"));
        }
    }

    /** Mesmo contrato de JdbcJobStore#findAllAnnotationSourced — PROGRAMMATIC nunca sai do cursor. */
    @Test
    void findAllAnnotationSourcedExcludesProgrammaticJobs() {
        store.upsert(definition("programmatic-job"));
        store.upsert(annotationSourcedDefinition("annotation-job"));

        try (Stream<StoredJob> annotationSourced = store.findAllAnnotationSourced()) {
            List<JobKey> keys = annotationSourced.map(job -> job.definition().key()).toList();
            assertThat(keys).containsExactly(JobKey.of("annotation-job"));
        }
    }

    @Test
    void markOrphanedSetsTheFlag() {
        JobKey key = JobKey.of("welcome-email");
        store.upsert(definition("welcome-email"));

        store.markOrphaned(key);

        assertThat(store.find(key)).map(StoredJob::orphaned).contains(true);
    }

    /** orphaned é dedução do sistema, não decisão de operador como paused — reupsert (a anotação reapareceu) limpa. */
    @Test
    void upsertClearsOrphanedOnReupsert() {
        JobKey key = JobKey.of("welcome-email");
        store.upsert(definition("welcome-email"));
        store.markOrphaned(key);

        store.upsert(definition("welcome-email"));

        assertThat(store.find(key)).map(StoredJob::orphaned).contains(false);
    }

    @Test
    void pauseAndResumeToggleTheFlag() {
        JobKey key = JobKey.of("welcome-email");
        store.upsert(definition("welcome-email"));

        store.pause(key);
        assertThat(store.find(key)).map(StoredJob::paused).contains(true);

        store.resume(key);
        assertThat(store.find(key)).map(StoredJob::paused).contains(false);
    }

    @Test
    void removeDeletesTheEntry() {
        JobKey key = JobKey.of("welcome-email");
        store.upsert(definition("welcome-email"));

        store.remove(key);

        assertThat(store.find(key)).isEmpty();
    }

    @Test
    void markOrphanedOnUnknownKeyIsANoOp() {
        store.markOrphaned(JobKey.of("ghost"));

        assertThat(store.find(JobKey.of("ghost"))).isEmpty();
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
    void tryIncrementRunningExecutionsOnUnknownKeyIsANoOp() {
        assertThat(store.tryIncrementRunningExecutions(JobKey.of("ghost"))).isFalse();
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
}
