/*
 * Copyright 2026 The Mohs Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
    @Test
    void invalidCatalogPublicationLeavesNoPartialDefinitionsAndRetainedContractsAreImmutable() {
        var catalog = new io.mohs.engine.ExecutableCatalog(java.util.Map.of(JobKey.of("a"), "a", JobKey.of("b"), "b"));
        var a = definition("a");
        var b = definition("b");
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> store.reconcile(catalog, List.of(a),
                io.mohs.engine.CatalogStore.Conflict.OVERRIDE)).isInstanceOf(IllegalArgumentException.class);
        assertThat(store.find(a.key())).isEmpty();
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> store.reconcile(catalog, List.of(a, a),
                io.mohs.engine.CatalogStore.Conflict.OVERRIDE)).isInstanceOf(IllegalArgumentException.class);
        store.reconcile(catalog, List.of(a, b), io.mohs.engine.CatalogStore.Conflict.OVERRIDE);
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> store.upsert(a.withInputContract("changed")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(store.find(a.key()).orElseThrow().definition()).isEqualTo(a);
    }


    record Handler() {
    }

    private static JobDefinition definition(String id) {
        return JobDefinition.of(id, new io.mohs.core.schedule.OnDemandSpec(), io.mohs.core.definition.JobOptions.defaults().withRunner("io"), io.mohs.core.schedule.Misfire.IGNORE, false);
    }

    private static JobDefinition definitionWithCap(String id, int max) {
        return JobDefinition.of(id, new io.mohs.core.schedule.OnDemandSpec(), io.mohs.core.definition.JobOptions.defaults().withMaxConcurrentExecutions(max), io.mohs.core.schedule.Misfire.IGNORE, false);
    }

    /** The public builder always stamps PROGRAMMATIC; only the canonical constructor yields ANNOTATION. */
    private static JobDefinition annotationSourcedDefinition(String id) {
        return new JobDefinition(JobKey.of(id), null,JobKey.of(id), new OnDemandSpec(),
                null, null, null, Misfire.IGNORE, false, 0, 0, null, null, DefinitionSource.ANNOTATION, JobKey.of(id).value());
    }

    private final InMemoryJobStore store = new InMemoryJobStore();

    @Test
    void upsertInsertsANewDefinition() {
        JobDefinition definition = definition("welcome-email");

        store.upsert(definition);
        Optional<StoredJob> stored = store.find(JobKey.of("welcome-email"));

        assertThat(stored).isPresent();
        assertThat(stored.get().definition()).isEqualTo(definition);
        assertThat(stored.get().paused()).isFalse();
    }

    /**
     * The trigger rule, exercised end to end: an upsert arms {@code nextFireAt} from the injected
     * clock, an unchanged schedule preserves it — so a redeploy does not shift the next firing —
     * and a changed one recomputes it.
     */
    @Test
    void upsertArmsPreservesAndRecomputesTheTrigger() {
        MutableClock clock = MutableClock.startingAt(Instant.parse("2026-08-15T12:00:00Z"));
        InMemoryJobStore clocked = new InMemoryJobStore(clock);
        JobDefinition every5m = JobDefinition.of("poll", new io.mohs.core.schedule.IntervalSpec(Duration.ofMinutes(5), false), io.mohs.core.definition.JobOptions.defaults(), io.mohs.core.schedule.Misfire.IGNORE, false);

        clocked.upsert(every5m);
        Instant armedAt = clocked.find(JobKey.of("poll")).orElseThrow().nextFireAt();
        assertThat(armedAt).isEqualTo(Instant.parse("2026-08-15T12:05:00Z"));

        clock.advance(Duration.ofMinutes(10));
        clocked.upsert(every5m);
        assertThat(clocked.find(JobKey.of("poll")).orElseThrow().nextFireAt()).isEqualTo(armedAt);

        clocked.upsert(JobDefinition.of("poll", new io.mohs.core.schedule.IntervalSpec(Duration.ofMinutes(1), false), io.mohs.core.definition.JobOptions.defaults(), io.mohs.core.schedule.Misfire.IGNORE, false));
        assertThat(clocked.find(JobKey.of("poll")).orElseThrow().nextFireAt())
                .isEqualTo(clock.instant().plus(Duration.ofMinutes(1)));
    }

    /**
     * {@code startPaused} is a birth property, not a standing instruction: it applies on first
     * registration, and after that {@code paused} belongs to the operator — a redeploy must never
     * re-pause a job somebody deliberately resumed.
     */
    @Test
    void startPausedAppliesOnlyAtBirth() {
        JobDefinition dormant = JobDefinition.of("dormant", new io.mohs.core.schedule.IntervalSpec(Duration.ofMinutes(1), false), io.mohs.core.definition.JobOptions.defaults(), io.mohs.core.schedule.Misfire.IGNORE, true);
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
        clocked.upsert(JobDefinition.of("due-late", new io.mohs.core.schedule.IntervalSpec(Duration.ofMinutes(1), false), io.mohs.core.definition.JobOptions.defaults(), io.mohs.core.schedule.Misfire.IGNORE, false));
        clock.advance(Duration.ofMinutes(2));
        clocked.upsert(JobDefinition.of("due-recent", new io.mohs.core.schedule.IntervalSpec(Duration.ofMinutes(1), false), io.mohs.core.definition.JobOptions.defaults(), io.mohs.core.schedule.Misfire.IGNORE, false));
        clocked.upsert(JobDefinition.of("not-due", new io.mohs.core.schedule.IntervalSpec(Duration.ofHours(6), false), io.mohs.core.definition.JobOptions.defaults(), io.mohs.core.schedule.Misfire.IGNORE, false));
        clocked.upsert(JobDefinition.of("import-file", new io.mohs.core.schedule.OnDemandSpec(), io.mohs.core.definition.JobOptions.defaults(), io.mohs.core.schedule.Misfire.IGNORE, false));
        clocked.upsert(JobDefinition.of("paused", new io.mohs.core.schedule.IntervalSpec(Duration.ofMinutes(1), false), io.mohs.core.definition.JobOptions.defaults(), io.mohs.core.schedule.Misfire.IGNORE, false));
        clocked.pause(JobKey.of("paused"));
        clock.advance(Duration.ofMinutes(2));

        assertThat(clocked.findDueRecurring(clock.instant(), 10).jobs())
                .extracting(stored -> stored.definition().key().value())
                .containsExactly("due-late", "due-recent");
        assertThat(clocked.findDueRecurring(clock.instant(), 1).jobs())
                .extracting(stored -> stored.definition().key().value())
                .containsExactly("due-late");
    }

    /**
     * Same semantics as the JDBC adapter: the schedule is swapped and the trigger recomputed,
     * while policies and operational state are left untouched.
     */
    @Test
    void rescheduleSwapsTheScheduleAndRearmsTheTrigger() {
        MutableClock clock = MutableClock.startingAt(Instant.parse("2026-08-15T12:00:00Z"));
        InMemoryJobStore clocked = new InMemoryJobStore(clock);
        clocked.upsert(JobDefinition.of("poll", new io.mohs.core.schedule.IntervalSpec(Duration.ofMinutes(5), false), io.mohs.core.definition.JobOptions.defaults().withRetries(3), io.mohs.core.schedule.Misfire.IGNORE, false));
        clocked.pause(JobKey.of("poll"));

        boolean rescheduled = clocked.reschedule(JobKey.of("poll"), new CronSpec("0 0 2 * * *", ZoneId.of("UTC")));

        assertThat(rescheduled).isTrue();
        StoredJob stored = clocked.find(JobKey.of("poll")).orElseThrow();
        assertThat(stored.definition().schedule()).isEqualTo(new CronSpec("0 0 2 * * *", ZoneId.of("UTC")));
        assertThat(stored.definition().retries()).isEqualTo(3);
        assertThat(stored.paused()).isTrue(); // operational state is not part of the schedule
        assertThat(stored.nextFireAt()).isEqualTo(Instant.parse("2026-08-16T02:00:00Z"));
        assertThat(clocked.reschedule(JobKey.of("ghost"), new OnDemandSpec())).isFalse();
    }


    @Test
    void upsertOnExistingKeyAppliesDefinitionalChanges() {
        store.upsert(definition("welcome-email"));

        JobDefinition changed = JobDefinition.of("welcome-email", new io.mohs.core.schedule.CronSpec("0 0 2 * * *", ZoneId.of("UTC")), io.mohs.core.definition.JobOptions.defaults().withRetries(5), io.mohs.core.schedule.Misfire.IGNORE, false);
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

}
