package io.mohs.test;

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
