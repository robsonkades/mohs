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
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.jspecify.annotations.Nullable;

import io.mohs.core.schedule.CronSpec;
import io.mohs.core.schedule.IntervalSpec;
import io.mohs.core.schedule.OnDemandSpec;
import io.mohs.core.schedule.Schedule;
import io.mohs.cron.CronExpression;

/**
 * Computes a {@link Schedule}'s next firing. It never reads the clock — the caller supplies
 * {@code reference} (the "now" from the engine's injected {@code Clock}); that rule is what makes
 * schedule tests deterministic, with no {@code Thread.sleep}.
 *
 * <p>For a {@link CronSpec} it delegates to the vendored {@link CronExpression} parser, caching the
 * parse result per expression — parsing is not expensive, but avoiding repeating it on every poll of
 * the claim loop is cheap mechanical sympathy.
 *
 * <p>For an {@link IntervalSpec} it adds the interval to {@code reference} — fixed-rate versus
 * fixed-delay is not this class's decision: the caller chooses what to pass as {@code reference} (the
 * scheduled time versus the end of the previous execution). An {@link OnDemandSpec} never fires by
 * itself.
 */
public final class NextFireCalculator {

    /**
     * Creates a schedule calculator with an empty cron cache.
     */
    public NextFireCalculator() {
    }

    /**
     * The expression cache's ceiling. The key is operator-controlled ({@code PATCH /jobs/{key}} to
     * {@code reschedule} with a new expression), so without a ceiling a loop of reschedules with
     * distinct expressions would grow the map forever. In practice it is bounded by the number of
     * jobs — the ceiling exists for the pathological case, and clearing everything is acceptable
     * because a miss costs a parse, not a query.
     */
    static final int MAX_CACHED_EXPRESSIONS = 10_000;

    private final Map<String, CronExpression> cronCache = new ConcurrentHashMap<>();

    /**
     * The next firing strictly after {@code reference}. Empty only for an {@link OnDemandSpec}.
     *
     * @param schedule the firing schedule to evaluate
     * @param reference the exclusive lower bound for the next firing
     * @return the next firing instant, or empty for an on-demand schedule
     * @throws IllegalArgumentException if the cron expression never fires (30 February, say) —
     *         syntactically valid, but unrealisable
     */
    public Optional<Instant> nextFireAfter(Schedule schedule, Instant reference) {
        return switch (schedule) {
            case CronSpec cron -> Optional.of(nextCronFire(cron, reference));
            case IntervalSpec interval -> Optional.of(reference.plus(interval.interval()));
            case OnDemandSpec _ -> Optional.empty();
        };
    }

    /**
     * The cron's next firing, with the two rules the expression alone does not guarantee.
     *
     * <p><b>Strict progress.</b> {@code CronExpression.next()} promises an instant strictly after the
     * seed, and for day-of-month {@code L-n} that promise has already been broken once. Its consumer
     * is {@code FiringPlanner.planSeries}, which ITERATES over the result: without progress it
     * materialises the same occurrence up to the cap of 1,440 and returns {@code next_fire_at}
     * unchanged — the trigger stays due forever and the job re-executes on every tick, with nothing
     * in the log. The root cause is fixed in {@code QuartzCronField}; this guard exists because an
     * invariant consumed by a loop cannot depend on the producer merely behaving. It is a loud
     * failure, which the firing path already knows how to route (an error in this job's plan, without
     * taking down the sweep of the others).
     *
     * <p><b>The DST fall-back.</b> At the end of daylight saving the same wall-clock time happens
     * TWICE, with different offsets, and the cron matches both — a "daily" 02:00 job executed twice on
     * the day of the transition. The repetition is suppressed: a loss is worse than a delay, and
     * duplicating a daily close is the worst possible outcome. The spring-forward gap is deliberately
     * NOT compensated — a time that does not exist does not fire, and the next occurrence is the
     * following day (an explicit decision, diverging from Quartz in that direction).
     */
    private Instant nextCronFire(CronSpec cron, Instant reference) {
        CronExpression parsed = parsedCron(cron.expression());
        ZonedDateTime seed = reference.atZone(cron.zone());
        ZonedDateTime next = requireAdvancing(parsed.next(seed), seed, cron);
        if (next.toLocalDateTime().equals(seed.toLocalDateTime())) {
            Duration ambiguity = Duration.between(seed, next);
            ZonedDateTime after = requireAdvancing(parsed.next(next), next, cron);
            // The repetition is only real work when the series is UNIFORMLY at least as dense as the
            // shift — on BOTH sides of the ambiguous slot. Looking only forward confuses "hourly cron"
            // with "twice a day, in adjacent hours": "0 0 2,3" has a 1h step AFTER 02:00 and no
            // occurrence BEFORE it, and duplicated the close. between(seed, next) IS the shift — the
            // same wall-clock time, a different offset.
            boolean denseAfter = Duration.between(next, after).compareTo(ambiguity) <= 0;
            ZonedDateTime before = parsed.next(seed.minus(ambiguity).minusNanos(1));
            boolean denseBefore = before != null && before.isBefore(seed);
            next = denseBefore && denseAfter ? next : after;
        }
        return next.toInstant();
    }

    /** The compiled expression, with the cache respecting {@link #MAX_CACHED_EXPRESSIONS}'s ceiling — see its Javadoc. */
    private CronExpression parsedCron(String expression) {
        if (cronCache.size() >= MAX_CACHED_EXPRESSIONS) {
            cronCache.clear();
        }
        return cronCache.computeIfAbsent(expression, CronExpression::parse);
    }

    private static ZonedDateTime requireAdvancing(@Nullable ZonedDateTime next, ZonedDateTime seed, CronSpec cron) {
        if (next == null) {
            throw new IllegalArgumentException("Cron expression never fires within the search bound: " + cron.expression());
        }
        if (!next.isAfter(seed)) {
            throw new IllegalArgumentException("Cron expression did not advance past " + seed + " (returned " + next
                    + "): " + cron.expression() + " — the series would never progress, and every tick would"
                    + " re-materialise the same occurrence; fix the expression");
        }
        return next;
    }
}
