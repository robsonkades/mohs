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

import java.time.Clock;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import io.mohs.core.definition.DefinitionSource;
import io.mohs.core.definition.JobDefinition;
import io.mohs.core.job.JobKey;
import io.mohs.core.schedule.Schedule;
import io.mohs.engine.JobStore;
import io.mohs.engine.NextFireCalculator;
import io.mohs.engine.StoredJob;

/**
 * In-memory {@link JobStore} — the "in-memory storage" implementation of the test kit
 * ({@code @MohsTest}).
 *
 * <p>It draws the same line between a job's DEFINITION (what it is: schedule, handler, policies)
 * and its OPERATIONAL state (what is true of it right now: paused, orphaned, when it fires next)
 * as {@code JdbcJobStore} (io.mohs.store.jdbc) — without touching a database at all, which is also
 * the proof that {@link JobStore} leaked nothing JDBC-specific.
 *
 * <p>The {@link Clock} constructor exists for trigger state: the upsert initialises
 * {@code nextFireAt} from the test's own deterministic clock ({@link MutableClock}). The no-arg
 * constructor uses the system clock, as a convenience for tests that do not exercise scheduling.
 *
 * <p>Two deliberate divergences from the JDBC adapter, documented rather than accidental:
 * <ol>
 *   <li>Healing a disarmed trigger is not implemented. With no {@code TriggerFirer} and no
 *       in-memory {@code HistoryStore}/{@code WorkQueue}, a disarmed recurring trigger is an
 *       unreachable state in this store, so the cure would be dead code; it arrives together with
 *       the kit's in-memory engine.</li>
 *   <li>{@link #remove} is a hard delete, whereas JDBC soft-retires and keeps the row. So here a
 *       post-remove "resurrection" is a birth and {@code startPaused} applies again, while in JDBC
 *       the row survives as the memory of the operator's decision and {@code paused} comes back as
 *       they left it.</li>
 * </ol>
 *
 * <p>Thread-safe: state lives in a {@link ConcurrentHashMap} and every mutation goes through an
 * atomic {@code compute}.
 */
public final class InMemoryJobStore implements JobStore {

    private final Map<JobKey, StoredJob> jobs = new ConcurrentHashMap<>();
    private final Clock clock;
    private final NextFireCalculator nextFireCalculator = new NextFireCalculator();

    public InMemoryJobStore() {
        this(Clock.systemUTC());
    }

    public InMemoryJobStore(Clock clock) {
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    /**
     * {@code orphaned} is always cleared here, whereas {@code paused} is preserved.
     *
     * <p>The asymmetry is deliberate: pause is an operator decision, while orphaned is an
     * inference by the system ("the annotation is gone") — and the upsert happening at all is
     * itself proof that a real source wants this job again. {@code nextFireAt} follows the trigger
     * rule: an unchanged schedule keeps it, so a redeploy does not silently shift the next firing;
     * a new or altered one recomputes it from the clock.
     *
     * @param definition the definition to store
     * @return the same definition, for call-site chaining
     */
    @Override
    public JobDefinition upsert(JobDefinition definition) {
        Objects.requireNonNull(definition, "definition");
        jobs.compute(definition.key(), (_, existing) -> {
            // startPaused only applies at birth: a re-upsert must not undo a resume the operator did
            boolean paused = existing != null ? existing.paused() : definition.startPaused();
            Instant nextFireAt = existing != null && existing.definition().schedule().equals(definition.schedule())
                    ? existing.nextFireAt()
                    : nextFireCalculator.nextFireAfter(definition.schedule(), clock.instant()).orElse(null);
            return new StoredJob(definition, false, paused, nextFireAt);
        });
        return definition;
    }

    @Override
    public Optional<StoredJob> find(JobKey key) {
        Objects.requireNonNull(key, "key");
        return Optional.ofNullable(jobs.get(key));
    }

    @Override
    public Stream<StoredJob> findAll() {
        return jobs.values().stream();
    }

    @Override
    public Stream<StoredJob> findAllAnnotationSourced() {
        return jobs.values().stream().filter(stored -> stored.definition().source() == DefinitionSource.ANNOTATION);
    }

    @Override
    public List<StoredJob> findDueRecurring(Instant now, int limit) {
        Objects.requireNonNull(now, "now");
        return jobs.values().stream()
                .filter(stored -> !stored.paused() && !stored.orphaned())
                .filter(stored -> stored.nextFireAt() != null && !stored.nextFireAt().isAfter(now))
                .sorted(Comparator.comparing(StoredJob::nextFireAt))
                .limit(limit)
                .toList();
    }

    @Override
    public void armNextFire(JobKey key, Instant nextFireAt) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(nextFireAt, "nextFireAt");
        jobs.computeIfPresent(key, (_, stored) -> stored.nextFireAt() != null
                ? stored
                : new StoredJob(stored.definition(), stored.orphaned(), stored.paused(), nextFireAt));
    }

    @Override
    public boolean reschedule(JobKey key, Schedule schedule) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(schedule, "schedule");
        // The remapping never returns null, so a non-null computeIfPresent result means the job existed
        return jobs.computeIfPresent(key, (_, stored) -> {
            JobDefinition current = stored.definition();
            JobDefinition redefined = new JobDefinition(current.key(), current.name(), current.handlerType(), schedule,
                    current.runner(), current.window(), current.rateLimit(), current.misfire(), current.startPaused(),
                    current.allowConcurrentExecutions(), current.maxConcurrentExecutions(), current.retries(),
                    current.timeout(), current.retryPolicy(), current.source());
            Instant nextFireAt = nextFireCalculator.nextFireAfter(schedule, clock.instant()).orElse(null);
            return new StoredJob(redefined, stored.orphaned(), stored.paused(), nextFireAt);
        }) != null;
    }

    @Override
    public void markOrphaned(JobKey key) {
        jobs.computeIfPresent(key, (_, stored) ->
                new StoredJob(stored.definition(), true, stored.paused(), stored.nextFireAt()));
    }

    @Override
    public void pause(JobKey key) {
        jobs.computeIfPresent(key, (_, stored) ->
                new StoredJob(stored.definition(), stored.orphaned(), true, stored.nextFireAt()));
    }

    @Override
    public void resume(JobKey key) {
        jobs.computeIfPresent(key, (_, stored) ->
                new StoredJob(stored.definition(), stored.orphaned(), false, stored.nextFireAt()));
    }

    @Override
    public void remove(JobKey key) {
        jobs.remove(key);
    }
}
