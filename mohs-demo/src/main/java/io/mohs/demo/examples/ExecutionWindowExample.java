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
package io.mohs.demo.examples;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.Month;
import java.time.ZoneOffset;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import io.mohs.core.definition.RecurringJob;
import io.mohs.core.resource.ExecutionWindow;

/**
 * <b>Scenario 13 — firing times that are excluded outright.</b>
 *
 * <p>A window is a named set of exclusions: a job whose scheduled time falls inside any of them does
 * not fire. It is the "not during the maintenance window", "not on weekends", "not on the days
 * accounting closes the month" rule, expressed once and shared by every job that obeys it.
 *
 * <p>It is <b>not</b> a delay and not a queue. An excluded occurrence is skipped, not postponed to
 * the edge of the window — so a window is right for work that is worthless outside its slot, and
 * wrong for work that must eventually happen. For the latter, keep the schedule and use a
 * {@link RateLimitExample rate limit} or a narrower cron.
 *
 * <h2>Beans only, and in UTC</h2>
 *
 * <p>Unlike runners and rate limits, a window has no property form: its exclusions are predicates,
 * and a predicate does not fit in YAML. This version evaluates every predicate against the
 * {@code Instant} in <b>UTC</b>, so "business hours" here means business hours in UTC — state the
 * offset you mean, rather than assuming the window follows the job's cron zone.
 *
 * <p>The builder's four forms cover most of what is asked for, and {@code exclude(Predicate)} covers
 * the rest — a holiday calendar loaded from the database, for instance. Keep the predicate cheap and
 * side-effect free: it is evaluated on the engine's path, per candidate occurrence.
 */
@Configuration(proxyBeanMethods = false)
public class ExecutionWindowExample {

    /**
     * Creates a {@code ExecutionWindowExample} instance.
     */
    public ExecutionWindowExample() {
    }

    private static final Logger log = LoggerFactory.getLogger(ExecutionWindowExample.class);

    /**
     * "Never while people are using the system." Weekends are out, the working day is out, and the
     * two dates finance closes the year are out — what remains are nights and holidays.
     *
     * <p>{@code excludeDaily} supports crossing midnight ({@code 22:00 -> 02:00} is a real overnight
     * window, not a silent no-op), and the interval is half-open: {@code [from, to)}.
     */
    @Bean
    ExecutionWindow exampleOffHours() {
        return ExecutionWindow.named("example-off-hours")
                .excludeWeekends()
                .excludeDaily(LocalTime.of(8, 0), LocalTime.of(20, 0))
                .excludeDates(List.of(
                        LocalDate.of(2026, Month.DECEMBER, 31),
                        LocalDate.of(2027, Month.JANUARY, 1)))
                .build();
    }

    /**
     * The escape hatch, for a rule that is genuinely yours: nothing on the first three days of a
     * month, while the previous month is being closed.
     */
    @Bean
    ExecutionWindow exampleMonthEndFreeze() {
        return ExecutionWindow.named("example-month-end-freeze")
                .exclude(instant -> instant.atZone(ZoneOffset.UTC).getDayOfMonth() <= 3)
                .build();
    }

    /**
     * Fires every ten minutes on paper; in practice only outside working hours, weekends and the two
     * frozen dates. The window narrows an existing schedule — it never invents an occurrence.
     */
    @RecurringJob(
            id = "example-reindex",
            every = "PT10M",
            window = "example-off-hours"
    )
    void reindex() {
        log.info("reindexing, because nobody is looking");
    }

    @RecurringJob(
            id = "example-archive-ledger",
            cron = "0 0 1 * * *",
            zone = "UTC",
            window = "example-month-end-freeze"
    )
    void archiveLedger() {
        log.info("archiving — never during the first three days of a month");
    }
}
