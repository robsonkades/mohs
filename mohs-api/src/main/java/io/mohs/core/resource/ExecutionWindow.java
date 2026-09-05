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
package io.mohs.core.resource;

import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;

/**
 * A firing exclusion window: a job whose scheduled time falls inside any configured exclusion does
 * not fire. Predicates exist only in code — there is no property-based equivalent, unlike
 * {@link MohsRunner}.
 *
 * <p>This first version's predicates evaluate the {@link Instant} in UTC. Whether an exclusion
 * should respect the job's own zone is a decision for the engine consuming this window, not for
 * this contract.
 *
 * <p><b>The record's generated {@code equals()}/{@code hashCode()} are, in practice, identity-based.</b>
 * {@code exclusions} is a list of {@link Predicate}, and every call to
 * {@link Builder#excludeWeekends()}/{@link Builder#excludeDaily}/{@link Builder#excludeDates}/
 * {@link Builder#exclude} creates a new, distinct lambda — so two windows built from exactly the
 * same calls are never {@code equals()}. True value semantics would require modelling the
 * exclusions as sealed data ({@code Weekends}/{@code Dates}/{@code DailyRange}/{@code Custom})
 * rather than raw predicates: a larger change, out of scope while nothing depends on equality or
 * deduplication of this type.
 */
public record ExecutionWindow(String name, List<Predicate<Instant>> exclusions) {

    public ExecutionWindow {
        Fields.requireNotBlank(name, "name");
        exclusions = List.copyOf(exclusions); // a defensive copy (Effective Java, Item 50)
    }

    /** {@code true} if the instant falls inside any configured exclusion. */
    public boolean excludes(Instant instant) {
        return exclusions.stream().anyMatch(exclusion -> exclusion.test(instant));
    }

    public static Builder named(String name) {
        return new Builder(name);
    }

    public static final class Builder {
        private final String name;
        private final List<Predicate<Instant>> exclusions = new ArrayList<>();

        private Builder(String name) {
            this.name = name;
        }

        public Builder excludeWeekends() {
            exclusions.add(instant -> {
                DayOfWeek day = inUtc(instant).getDayOfWeek();
                return day == DayOfWeek.SATURDAY || day == DayOfWeek.SUNDAY;
            });
            return this;
        }

        public Builder excludeDates(Collection<LocalDate> dates) {
            Set<LocalDate> copy = Set.copyOf(dates);
            exclusions.add(instant -> copy.contains(inUtc(instant).toLocalDate()));
            return this;
        }

        /**
         * Excludes the half-open daily interval {@code [from, to)}, in UTC.
         *
         * <p>It supports crossing midnight (for example {@code excludeDaily(22:00, 02:00)} for an
         * overnight maintenance window): when {@code from} is after {@code to}, the interval is
         * read as {@code [from, 24:00) union [00:00, to)} instead of silently becoming a no-op.
         * {@code from} equal to {@code to} remains empty by definition, as any half-open interval
         * {@code [t, t)} is.
         */
        public Builder excludeDaily(LocalTime from, LocalTime to) {
            Objects.requireNonNull(from, "from");
            Objects.requireNonNull(to, "to");
            if (from.equals(to)) {
                exclusions.add(instant -> false);
            } else if (from.isBefore(to)) {
                exclusions.add(instant -> {
                    LocalTime time = inUtc(instant).toLocalTime();
                    return !time.isBefore(from) && time.isBefore(to);
                });
            } else {
                exclusions.add(instant -> {
                    LocalTime time = inUtc(instant).toLocalTime();
                    return !time.isBefore(from) || time.isBefore(to);
                });
            }
            return this;
        }

        /** The one place the contract's "evaluated in UTC" lives — every exclusion reads the instant through it. */
        private static ZonedDateTime inUtc(Instant instant) {
            return instant.atZone(ZoneOffset.UTC);
        }

        public Builder exclude(Predicate<Instant> predicate) {
            exclusions.add(Objects.requireNonNull(predicate, "predicate"));
            return this;
        }

        public ExecutionWindow build() {
            return new ExecutionWindow(name, exclusions);
        }
    }
}
