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

import io.mohs.core.definition.DefinitionSource;
import io.mohs.core.definition.JobDefinition;
import io.mohs.core.job.JobKey;
import io.mohs.core.schedule.Schedule;

/**
 * Persistence of {@link JobDefinition} — a Repository (PoEAA), the port {@code io.mohs.store.jdbc}
 * implements (a Data Mapper).
 *
 * <p>{@link #upsert} writes definitional state. The operational fields ({@code orphaned},
 * {@code paused}) have two deliberate exceptions: the upsert clears {@code orphaned} (the source
 * reappearing is proof of life) and, ONLY on the first registration, initialises
 * {@code paused = startPaused}; after birth, {@code paused} belongs exclusively to
 * {@link #pause}/{@link #resume}.
 */
public interface JobStore {

    /**
     * The upsert owns the trigger's initial state: a new or altered schedule arms {@code nextFireAt}
     * recomputed from the clock; an unchanged schedule preserves the stored value — and preserving
     * means <b>not writing</b> the column, never rewriting the value that was read (a lost update
     * against the firing CAS and the completion's rearm); an on-demand job stays disarmed
     * ({@code null}).
     *
     * <p>An unchanged recurring schedule with a disarmed trigger is healed (rearmed) — for
     * fixed-delay, only when there is no live scheduler occurrence.
     */
    JobDefinition upsert(JobDefinition definition);

    Optional<StoredJob> find(JobKey key);

    /**
     * A stream over an open cursor — it does not materialise the whole table in memory at once. The
     * caller owns the lifecycle (try-with-resources); closing the stream releases the connection
     * behind it.
     *
     * <p>On Postgres this only holds if the call runs inside a transaction (autocommit off) — outside
     * one, the driver materialises the entire result before returning the first item, despite the
     * {@code fetchSize} configured on the template's side.
     */
    Stream<StoredJob> findAll();

    /**
     * The same cursor contract as {@link #findAll()}, filtered to {@link DefinitionSource#ANNOTATION}
     * at the source (not in memory afterwards) — {@code io.mohs.autoconfigure.MohsJobScanner}
     * reconciles orphans only against this subset; {@code PROGRAMMATIC} definitions never become
     * {@code ORPHANED} (see {@link #markOrphaned}), so fetching them along would be read bandwidth
     * with no use.
     */
    Stream<StoredJob> findAllAnnotationSourced();

    /**
     * Due recurring jobs: {@code next_fire_at <= now}, excluding {@code paused}, {@code orphaned} and
     * {@code retired} — a pause blocks exactly the trigger, and on-demand still works while paused.
     * Oldest first, at most {@code limit} (the surplus stays due and drains over the following ticks).
     */
    List<StoredJob> findDueRecurring(Instant now, int limit);

    /**
     * Arms {@code next_fire_at} only if it is disarmed ({@code NULL}) — the fixed-delay chain's cures:
     * a cancelled occurrence still {@code ENQUEUED} never goes through the completion path that
     * rearms, and this guard guarantees the cure never clobbers a series a schedule-change upsert has
     * already rearmed.
     */
    void armNextFire(JobKey key, Instant nextFireAt);

    /**
     * Rewrites the schedule and rearms the trigger, recomputed from the clock, in the SAME write — the
     * only legitimate schedule write besides {@link #upsert}.
     *
     * <p>The write of {@code next_fire_at} is unconditional on purpose: "an explicit reconfiguration
     * beats a concurrent firing". Guarded by {@code retired} — a retired job is invisible to the whole
     * API.
     *
     * @return {@code true} if the row existed (and was not retired)
     */
    boolean reschedule(JobKey key, Schedule schedule);

    /** An {@code ANNOTATION} job present in the store and absent from the code — it does not fire, and it does not erase history. */
    void markOrphaned(JobKey key);

    void pause(JobKey key);

    void resume(JobKey key);

    /**
     * Explicit retirement ({@code Mohs#remove}) — only for {@code PROGRAMMATIC} definitions (the
     * caller, {@code MohsImpl}, is what validates the {@code source}).
     *
     * <p>A soft retire, never a delete: it drains the queue ({@code mohs_ready}), cancelling whatever
     * was enqueued, and marks the definition {@code retired} — it disappears from
     * {@link #find}/{@link #findAll}, but the row (and the whole execution history) remains. An
     * {@link #upsert} of the same {@code job_key} resurrects the definition.
     *
     * <p>The concurrency slot is not this port's business: the cap derives from {@code mohs_lease} —
     * deleting the lease IS releasing the slot.
     */
    void remove(JobKey key);
}
