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

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import io.mohs.core.definition.RecurringJob;
import io.mohs.core.schedule.Misfire;

/**
 * <b>Scenario 1 — a job that fires on a calendar.</b> The first thing most applications need: run
 * this at 03:00, run that on weekday mornings.
 *
 * <p>Cron expressions are Quartz-style and <b>seconds-first</b> (six fields), so {@code 0 0 3 * * *}
 * is "at 03:00:00", not "every three minutes past". {@code L}, {@code W} and {@code #} are
 * supported.
 *
 * <h2>The zone is mandatory, and that is the point</h2>
 *
 * <p>{@code zone} has no default. A schedule that silently follows the JVM's default zone is a job
 * that moves an hour when a container is redeployed to another region, and moves again twice a year
 * for daylight saving — which is precisely the kind of failure nobody attributes to the scheduler.
 * Naming an IANA zone makes the contract explicit and identical on every node of the cluster.
 *
 * <p>DST is honoured: a daily job does not fire twice on the fall-back day. The spring-forward gap
 * is a deliberate divergence from Quartz — an occurrence inside the missing hour is skipped rather
 * than compensated.
 *
 * <h2>What happens when a firing is missed</h2>
 *
 * <p>A node was down, the engine was paused, the clock jumped. Later than
 * {@code mohs.engine.misfire-threshold} (60s by default), the firing counts as MISSED and
 * {@link Misfire} decides:
 *
 * <ul>
 * <li>{@link Misfire#IGNORE} (the default) — skip it, resume at the next regular occurrence. Right
 *     for anything idempotent-by-recency: a cache refresh does not want yesterday's run.</li>
 * <li>{@link Misfire#FIRE_NOW} — fire once, immediately, for the most recent missed occurrence.
 *     Right for a report that must exist, even late.</li>
 * <li>{@link Misfire#FIRE_ALL_MISSED} — replay every missed occurrence, capped at 1,440 per job per
 *     cycle and drained rather than discarded. Right only when each occurrence does distinct work
 *     (per-day billing, say). Be sure the handler can take the burst.</li>
 * </ul>
 *
 * <h2>A recurring handler never receives a typed payload</h2>
 *
 * <p>An automatic occurrence carries no data — there is nobody to supply it. The boot therefore
 * rejects a recurring handler that declares, say, an {@code Invoice} parameter, rather than letting
 * every occurrence fail at runtime. {@code Map} and {@code Object} are allowed, because the same job
 * can also be invoked manually with a payload: see {@link #weekdayDigest}.
 *
 * @see IntervalJobExample for the other kind of automatic trigger
 */
@Component
public class CronJobExample {

    private static final Logger log = LoggerFactory.getLogger(CronJobExample.class);

    /**
     * 03:00 in São Paulo — which is a different instant from 03:00 in UTC, and stays 03:00 local
     * across the two DST transitions.
     *
     * <p>{@code FIRE_NOW} because an invoice run that missed its window still has to happen; the
     * three retries are for the transient failure of whatever it talks to.
     */
    @RecurringJob(
            id = "example-nightly-invoices",
            name = "Nightly invoices",
            cron = "0 0 3 * * *",
            zone = "America/Sao_Paulo",
            misfire = Misfire.FIRE_NOW,
            retries = 3,
            timeout = "PT30M"
    )
    void nightlyInvoices() {
        log.info("closing yesterday's invoices");
    }

    /**
     * Weekday mornings only. {@code startPaused} means the definition is registered but disarmed at
     * BIRTH: nothing fires until an explicit resume ({@code Mohs.resume} or
     * {@code POST /jobs/{id}/resume}). It applies to the first registration only — a redeploy never
     * re-pauses a job an operator has resumed.
     *
     * <p>The {@code Map} parameter is what lets the same job be invoked by hand with data
     * ({@code mohs.schedule("example-weekday-digest", Map.of("audience", "ops")).now()}); an
     * automatic occurrence delivers an empty map.
     */
    @RecurringJob(
            id = "example-weekday-digest",
            cron = "0 30 8 * * MON-FRI",
            zone = "UTC",
            startPaused = true
    )
    void weekdayDigest(Map<String, Object> parameters) {
        log.info("digest for audience {}", parameters.getOrDefault("audience", "everyone"));
    }
}
