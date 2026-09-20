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
import io.mohs.engine.JobBinding;
import io.mohs.engine.NextFireCalculator;
import io.mohs.engine.StoredJob;

/**
 * Deterministic definition and catalog fixture. Retains retired identities, pause decisions and schedule revisions.
 * It has no distributed sessions, queue, history or transaction manager; JDBC protocol tests cover those guarantees.
 */
public final class InMemoryJobStore implements JobStore, io.mohs.engine.CatalogStore {

    private final Map<JobKey, StoredJob> jobs = new ConcurrentHashMap<>();
    private final Map<JobKey, JobBinding> bindings = new ConcurrentHashMap<>();
    private final Map<JobKey, StoredJob> retired = new ConcurrentHashMap<>();

    @Override
    public synchronized void reconcile(io.mohs.engine.ExecutableCatalog catalog, List<JobDefinition> definitions, Conflict conflict) {
        definitions = List.copyOf(definitions);
        Objects.requireNonNull(conflict, "conflict");
        catalog.requireDefinitions(definitions);
        var staged = new java.util.LinkedHashMap<JobKey, JobDefinition>();
        for (var incoming : definitions) {
            catalog.require(incoming.key(), incoming.inputContract());
            var previous = bindings.get(incoming.key());
            if (previous != null && !previous.executableKey().equals(incoming.key())) {
                throw new IllegalStateException("executable collides with a retained named recurrence");
            }
            var existing = jobs.get(incoming.key());
            if (existing == null && previous != null) {
                var retained = retired.get(incoming.key());
                if (retained != null) catalog.require(incoming.key(), retained.definition().inputContract());
                continue;
            }
            if (existing != null) {
                catalog.require(incoming.key(), existing.definition().inputContract());
                if (!existing.definition().equals(incoming)) {
                    if (conflict == Conflict.FAIL) throw new IllegalStateException("definition diverged from the stored one");
                    if (conflict == Conflict.PRESERVE) continue;
                }
            }
            staged.put(incoming.key(), incoming);
        }
        for (var existing : jobs.values()) {
            catalog.require(existing.definition().executableKey(), existing.definition().inputContract());
        }
        staged.values().forEach(this::upsert);
    }

    @Override
    public synchronized StoredJob upsertBound(JobDefinition definition, JobBinding binding) {
        if (!definition.key().equals(binding.key())) throw new IllegalArgumentException("binding key differs from definition");
        if (!binding.key().equals(binding.executableKey())) {
            if (find(binding.executableKey()).isEmpty()) {
                throw new IllegalArgumentException("no active executable '" + binding.executableKey().value() + "'");
            }
            JobBinding target = bindings.get(binding.executableKey());
            if (target != null && target.input() != null) {
                throw new IllegalArgumentException("a recurrence cannot reference another recurrence");
            }
        }
        JobBinding previous = bindings.get(binding.key());
        StoredJob existing = jobs.get(binding.key());
        if (previous != null && !previous.executableKey().equals(binding.executableKey())
                || existing != null && (previous == null && !binding.key().equals(binding.executableKey()))) {
            throw new IllegalArgumentException("job '" + binding.key().value() + "' belongs to another definition or executable");
        }
        definition = definition.withExecutableKey(binding.executableKey());
        if (binding.input() != null) definition = definition.withInputContract(binding.input().inputContract());
        upsert(definition);
        bindings.put(binding.key(), binding);
        return jobs.get(definition.key());
    }

    @Override
    public synchronized Optional<JobBinding> findBinding(JobKey key) {
        return Optional.ofNullable(bindings.get(key));
    }

    @Override
    public synchronized List<JobBinding> bindings() {
        return bindings.values().stream().filter(binding -> jobs.containsKey(binding.key())).toList();
    }
    private final Clock clock;
    private final NextFireCalculator nextFireCalculator = new NextFireCalculator();

    /**
     * Creates a {@code InMemoryJobStore} with the supplied values.
     */
    public InMemoryJobStore() {
        this(Clock.systemUTC());
    }

    /**
     * Creates a {@code InMemoryJobStore} with the supplied values.
     *
     * @param clock the time source used by the component
     */
    public InMemoryJobStore(Clock clock) {
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    /** Updates an active definition, preserving operator pause and unchanged trigger state. */
    @Override
    public synchronized JobDefinition upsert(JobDefinition definition) {
        Objects.requireNonNull(definition, "definition");
        if (retired.containsKey(definition.key())) throw new IllegalStateException("job is retired; use reactivate before updating it");
        jobs.compute(definition.key(), (_, existing) -> {
            if (existing != null && !existing.definition().inputContract().equals(definition.inputContract())) {
                throw new IllegalArgumentException("input contract is immutable for a retained job identity");
            }
            // startPaused only applies at birth: a re-upsert must not undo a resume the operator did
            boolean paused = existing != null ? existing.paused() : definition.startPaused();
            Instant nextFireAt = existing != null && existing.definition().schedule().equals(definition.schedule())
                    ? existing.nextFireAt()
                    : nextFireCalculator.nextFireAfter(definition.schedule(), clock.instant()).orElse(null);
            return new StoredJob(definition, paused, nextFireAt, existing == null ? 1L : existing.definition().schedule().equals(definition.schedule())
                    ? existing.scheduleRevision() : existing.scheduleRevision() + 1);
        });
        bindings.putIfAbsent(definition.key(), new JobBinding(definition.key(), definition.executableKey(), null));
        return definition;
    }

    @Override
    public synchronized Optional<StoredJob> find(JobKey key) {
        Objects.requireNonNull(key, "key");
        return Optional.ofNullable(jobs.get(key));
    }

    @Override
    public List<StoredJob> findByKeys(java.util.Collection<JobKey> keys) {
        return keys.stream().distinct().map(jobs::get).filter(Objects::nonNull).toList();
    }

    @Override
    public Optional<Instant> nextFireAfter(Instant now) {
        return jobs.values().stream().filter(job -> !job.paused()).map(StoredJob::nextFireAt)
                .filter(Objects::nonNull).filter(instant -> instant.isAfter(now)).min(Instant::compareTo);
    }

    @Override
    public Stream<StoredJob> findAll() {
        return jobs.values().stream();
    }


    @Override
    public DueJobs findDueRecurring(Instant horizon, int limit) {
        Objects.requireNonNull(horizon, "horizon");
        if (limit < 1 || limit > 1000) throw new IllegalArgumentException("due page limit must be in [1, 1000]");
        Instant sample = clock.instant();
        Instant now = io.mohs.core.TimePrecision.observed(horizon.isBefore(sample) ? horizon : sample);
        return new DueJobs(now, jobs.values().stream()
                .filter(stored -> !stored.paused())
                .filter(stored -> stored.nextFireAt() != null && !stored.nextFireAt().isAfter(now))
                .sorted(Comparator.comparing(StoredJob::nextFireAt))
                .limit(limit).toList());
    }


    @Override
    public boolean reschedule(JobKey key, Schedule schedule) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(schedule, "schedule");
        // The remapping never returns null, so a non-null computeIfPresent result means the job existed
        return jobs.computeIfPresent(key, (_, stored) -> {
            JobDefinition current = stored.definition();
            JobDefinition redefined = new JobDefinition(current.key(), current.name(), current.executableKey(), schedule,
                    current.runner(), current.window(), current.rateLimit(), current.misfire(), current.startPaused(), current.maxConcurrentExecutions(), current.retries(),
                    current.timeout(), current.retryPolicy(), current.source(), current.inputContract());
            Instant nextFireAt = nextFireCalculator.nextFireAfter(schedule, clock.instant()).orElse(null);
            return new StoredJob(redefined, stored.paused(), nextFireAt, stored.scheduleRevision() + 1);
        }) != null;
    }


    @Override
    public void pause(JobKey key) {
        jobs.computeIfPresent(key, (_, stored) ->
                new StoredJob(stored.definition(), true, stored.nextFireAt(), stored.scheduleRevision()));
    }

    @Override
    public void resume(JobKey key) {
        jobs.computeIfPresent(key, (_, stored) ->
                new StoredJob(stored.definition(), false, stored.nextFireAt(), stored.scheduleRevision()));
    }

    @Override
    public synchronized void remove(JobKey key) {
        if (bindings().stream().anyMatch(binding -> binding.executableKey().equals(key) && !binding.key().equals(key))) {
            throw new IllegalArgumentException("remove the executable's recurring schedules first");
        }
        var previous = jobs.remove(key);
        if (previous != null) retired.put(key, new StoredJob(previous.definition(), previous.paused(),
                null, previous.scheduleRevision() + 1));
    }

    @Override
    public synchronized boolean reactivate(JobKey key, io.mohs.engine.ExecutableCatalog catalog) {
        var stored = retired.get(key);
        if (stored == null) return false;
        var definition = stored.definition();
        catalog.require(definition.executableKey(), definition.inputContract());
        if (!key.equals(definition.executableKey()) && find(definition.executableKey()).isEmpty()) {
            throw new IllegalStateException("reactivate the executable before its named recurrences");
        }
        var next = nextFireCalculator.nextFireAfter(definition.schedule(), clock.instant()).orElse(null);
        jobs.put(key, new StoredJob(definition, stored.paused(), next, stored.scheduleRevision() + 1));
        retired.remove(key);
        return true;
    }
}
