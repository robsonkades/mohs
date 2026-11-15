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
package io.mohs.engine;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import io.mohs.core.definition.JobDefinition;
import io.mohs.core.job.JobKey;
import io.mohs.core.schedule.Schedule;

/** Complete definition operations. Catalog registration, firing and execution completion have separate atomic ports. */
public interface JobStore {

    /** Atomically persists a definition and direct executable binding, refusing ownership collisions. */
    StoredJob upsertBound(JobDefinition definition, JobBinding binding);

    /** Reads a binding, including retained bindings of retired definitions. */
    Optional<JobBinding> findBinding(JobKey key);

    /** Resolves executable identity without loading the recurring input; includes retained bindings. */
    default Optional<JobKey> findExecutableKey(JobKey key) {
        return findBinding(key).map(JobBinding::executableKey);
    }

    /** Whether retiring this executable would leave an active recurrence without its owner. */
    default boolean hasActiveDependents(JobKey key) {
        return bindings().stream().anyMatch(binding -> binding.executableKey().equals(key)
                && !binding.key().equals(key));
    }

    /** Active bindings for startup contract validation; a detached snapshot. */
    List<JobBinding> bindings();

    /**
     * Writes an active retained definition. Before any catalog is installed this also seeds standalone compositions.
     * New executable identities after catalog publication require {@link CatalogStore#reconcile}.
     * Unchanged schedules preserve nextFireAt and the revision; changed schedules start a new series.
     * A retired identity requires explicit {@link #reactivate}; input contracts and executable bindings remain immutable.
     * @param definition the desired active definition
     * @return the persisted definition
     */
    JobDefinition upsert(JobDefinition definition);

    /**
     * Looks up a stored job and its trigger state.
     *
     * @param key the stable identity of the job
     * @return the stored job, or empty when the key does not exist
     */
    Optional<StoredJob> find(JobKey key);

    /**
     * A stream over an open cursor — it does not materialise the whole table in memory at once. The
     * caller owns the lifecycle (try-with-resources); closing the stream releases the connection
     * behind it.
     *
     * <p>On Postgres this only holds if the call runs inside a transaction (autocommit off) — outside
     * one, the driver materialises the entire result before returning the first item, despite the
     * {@code fetchSize} configured on the template's side.
     *
     * @return an open cursor stream that the caller must close
     */
    Stream<StoredJob> findAll();

    /** Detached policies for the supplied identities, excluding retired definitions and input payloads. */
    List<StoredJob> findByKeys(java.util.Collection<JobKey> keys);

    /** Nearest active, unpaused trigger strictly after this observation; an advisory sleep horizon. */
    Optional<Instant> nextFireAfter(Instant now);


    /** A bounded due page and the exact cutoff used by its query and misfire planning. */
    record DueJobs(Instant observedAt, List<StoredJob> jobs) {
        public DueJobs {
            java.util.Objects.requireNonNull(observedAt, "observedAt");
            jobs = List.copyOf(jobs);
        }
    }

    /**
     * Returns active, unpaused triggers in time order, at most limit. The observation is the earlier
     * of the requested horizon and a fresh store-clock sample: a caller can narrow eligibility but
     * cannot make a future trigger due. Planning must use the returned observation, not the caller clock.
     */
    DueJobs findDueRecurring(Instant horizon, int limit);


    /**
     * Rewrites the schedule and rearms the trigger, recomputed from the clock, in the SAME write — the
     * only legitimate schedule write besides {@link #upsert}.
     *
     * <p>The write of {@code next_fire_at} is unconditional on purpose: "an explicit reconfiguration
     * beats a concurrent firing". Guarded by {@code retired} — a retired job is invisible to the whole
     * API.
     *
     * @param key the stable identity of the job
     * @param schedule the replacement firing schedule
     * @return {@code true} if the row existed (and was not retired)
     */
    boolean reschedule(JobKey key, Schedule schedule);


    /**
     * Suspends automatic firing for the identified job.
     *
     * @param key the stable identity of the job
     */
    void pause(JobKey key);

    /**
     * Resumes automatic firing for the identified job.
     *
     * @param key the stable identity of the job
     */
    void resume(JobKey key);

    /**
     * Retires an annotated or programmatic definition and cancels its queued executions in one transaction.
     * Active named recurrences must be retired before their executable. Acquired work may complete;
     * automatic retry is suppressed while the definition is retired. History and identity remain retained.
     * A joined host transaction must use READ_COMMITTED so the locked scan includes committed work.
     * @param key the definition to retire; unknown or already retired is a no-op
     */
    void remove(JobKey key);

    /** Explicitly reactivates a retained definition under its catalog and a new schedule revision. */
    boolean reactivate(JobKey key, ExecutableCatalog catalog);
}
