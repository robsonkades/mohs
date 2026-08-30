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

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.jspecify.annotations.Nullable;

import io.mohs.core.schedule.CronSpec;
import io.mohs.core.schedule.IntervalSpec;
import io.mohs.core.schedule.Misfire;
import io.mohs.core.schedule.OnDemandSpec;
import io.mohs.core.schedule.Schedule;

/**
 * Decides what a due trigger fires — a pure function over ({@code schedule}, {@code misfire},
 * {@code next_fire_at}, {@code now}): no I/O and no clock read; the caller supplies the "now" from
 * the injected {@code Clock}, the same rule as {@link NextFireCalculator}.
 *
 * <p>A <b>missed</b> occurrence is one older than {@code misfireThreshold} — one due within the
 * threshold fires late under any policy (a delay of up to one poll interval is normal operation, not
 * a failure). Only the batch of missed ones answers to the policy: {@link Misfire#IGNORE} discards,
 * {@link Misfire#FIRE_NOW} compensates with a single immediate firing, and
 * {@link Misfire#FIRE_ALL_MISSED} replays each. Skipping over missed occurrences never walks them one
 * by one (cron recomputes from the threshold; fixed-rate jumps by arithmetic, preserving the series'
 * anchor).
 *
 * <p>Fixed-delay ({@code afterFinish}) is a chain of single occurrences: once materialised, the next
 * firing is unknown until the execution finishes (the plan's {@code nextFireAt} comes out
 * {@code null} — the completion rearms it, see {@code LeaseStore.CompletionResult#rearmNextFireAt}).
 */
public final class FiringPlanner {

    /**
     * The ceiling on occurrences materialised per job per cycle — "replay capped at 1,440 per cycle,
     * drained, never discarded"; applied to every materialisation, not only replay: a pathological
     * schedule (a millisecond interval) must not turn one tick into an unbounded insert. When capped,
     * the plan's {@code nextFireAt} stays due and the surplus drains over the following ticks.
     */
    static final int MAX_OCCURRENCES_PER_CYCLE = 1440;

    private final NextFireCalculator calculator;
    private final Duration misfireThreshold;

    public FiringPlanner(NextFireCalculator calculator, Duration misfireThreshold) {
        this.calculator = Objects.requireNonNull(calculator, "calculator");
        this.misfireThreshold = Objects.requireNonNull(misfireThreshold, "misfireThreshold");
        if (!misfireThreshold.isPositive()) {
            throw new IllegalArgumentException("misfireThreshold must be positive, got " + misfireThreshold);
        }
    }

    /**
     * @param nextFireAt the observed {@code next_fire_at} — due ({@code <= now}) by the caller's
     *        contract ({@code JobStore#findDueRecurring})
     * @throws IllegalArgumentException for an {@link OnDemandSpec} (never due — a caller bug) or an
     *         unrealisable cron (one that never fires)
     */
    public Plan plan(Schedule schedule, Misfire misfire, Instant nextFireAt, Instant now) {
        Objects.requireNonNull(schedule, "schedule");
        Objects.requireNonNull(misfire, "misfire");
        Objects.requireNonNull(nextFireAt, "nextFireAt");
        Objects.requireNonNull(now, "now");
        // The 1ms slack is the COLUMN's resolution, not clock tolerance: DATETIME2 (100ns) and
        // DATETIME(6) (microseconds) round the next_fire_at the calculation produced with nanoseconds,
        // and the SELECT may return a row whose read value comes back marginally after the raw now.
        // Without the slack, a non-event becomes a log.error — and a benign ERROR is what erodes trust
        // in the log at 3 a.m.
        // (The database-synced clock does NOT reach this guard: fireDueTriggers reads the clock once
        // and passes the SAME now to findDueRecurring and to plan. The real violation would be a
        // custom JobStore ignoring the now it was given.)
        if (nextFireAt.isAfter(now.plusMillis(1))) {
            throw new IllegalArgumentException("trigger is not due yet: next_fire_at=" + nextFireAt + " is after now="
                    + now + " — the caller must only plan triggers already due (" + schedule + ")");
        }
        return switch (schedule) {
            case OnDemandSpec _ -> throw new IllegalArgumentException("on-demand schedules are never due");
            case IntervalSpec interval when interval.afterFinish() -> planAfterFinish(interval, misfire, nextFireAt, now);
            case IntervalSpec _, CronSpec _ -> planSeries(schedule, misfire, nextFireAt, now);
        };
    }

    private Plan planAfterFinish(IntervalSpec schedule, Misfire misfire, Instant nextFireAt, Instant now) {
        if (!missed(nextFireAt, now)) {
            return new Plan(List.of(nextFireAt), null, false);
        }
        return switch (misfire) {
            case IGNORE -> new Plan(List.of(), now.plus(schedule.interval()), true);
            // Only one occurrence can be missed in an end-to-start chain — both policies coincide
            case FIRE_NOW, FIRE_ALL_MISSED -> new Plan(List.of(now), null, true);
        };
    }

    private Plan planSeries(Schedule schedule, Misfire misfire, Instant nextFireAt, Instant now) {
        boolean misfired = missed(nextFireAt, now);
        boolean compensate = misfired && misfire == Misfire.FIRE_NOW;
        // FIRE_ALL_MISSED replays from next_fire_at itself — nothing to skip
        Instant cursor = misfired && misfire != Misfire.FIRE_ALL_MISSED
                ? firstNotMissed(schedule, nextFireAt, now)
                : nextFireAt;
        // The compensation reserves its own slot in the cap — "the cap applies to every
        // materialisation"; the displaced occurrence stays due and drains
        int walkCap = compensate ? MAX_OCCURRENCES_PER_CYCLE - 1 : MAX_OCCURRENCES_PER_CYCLE;
        List<Instant> occurrences = new ArrayList<>();
        while (!cursor.isAfter(now) && occurrences.size() < walkCap) {
            occurrences.add(cursor);
            cursor = calculator.nextFireAfter(schedule, cursor)
                    .orElseThrow(() -> new IllegalStateException("recurring schedule stopped producing occurrences: " + schedule));
        }
        // The compensation belongs to the MISSED BATCH; when the series already placed an occurrence
        // exactly at now (a schedule aligned to the tick's instant), THAT one is already the immediate
        // firing — adding another writes two executions with the same scheduled_at, and nothing in the
        // schema stops both from running
        if (compensate && (occurrences.isEmpty() || !occurrences.getLast().equals(now))) {
            occurrences.add(now); // the missed batch's compensation — the most recent one, closing the chronological order
        }
        // The defensive copy belongs to Plan's canonical constructor — here it would be the second
        return new Plan(occurrences, cursor, misfired);
    }

    private boolean missed(Instant occurrence, Instant now) {
        return occurrence.isBefore(now.minus(misfireThreshold));
    }

    /**
     * The first non-missed occurrence ({@code >= now - threshold}), without walking the missed ones:
     * cron recomputes straight from the threshold (the series is absolute); fixed-rate jumps by
     * integer division, preserving the anchor — the next regular occurrence stays on the original
     * series, never re-anchored to the tick's instant.
     */
    private Instant firstNotMissed(Schedule schedule, Instant seriesAnchor, Instant now) {
        Instant boundary = now.minus(misfireThreshold);
        return switch (schedule) {
            // minusNanos(1): nextFireAfter is strictly-after, and an occurrence exactly at the threshold is not missed
            case CronSpec cron -> calculator.nextFireAfter(cron, boundary.minusNanos(1))
                    .orElseThrow(() -> new IllegalStateException("cron schedule stopped producing occurrences: " + cron));
            case IntervalSpec interval -> {
                Duration step = interval.interval();
                long steps = Duration.between(seriesAnchor, boundary).dividedBy(step);
                Instant candidate = seriesAnchor.plus(step.multipliedBy(steps));
                yield candidate.isBefore(boundary) ? candidate.plus(step) : candidate;
            }
            case OnDemandSpec _ -> throw new IllegalArgumentException("on-demand schedules are never due");
        };
    }

    /**
     * The planner's verdict for a due trigger: the instants to materialise (in chronological order;
     * each occurrence's {@code scheduled_at}), the new {@code next_fire_at} ({@code null} means
     * fixed-delay awaiting the end; {@code <= now} means a capped batch, still due) and whether any
     * occurrence was missed ({@code misfired} — the trigger for the caller's WARN).
     */
    public record Plan(List<Instant> occurrences, @Nullable Instant nextFireAt, boolean misfired) {

        public Plan {
            occurrences = List.copyOf(occurrences);
        }
    }
}
