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

import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import io.mohs.core.definition.RecurringJob;

/**
 * <b>Scenario 2 — a job that fires on a stopwatch.</b> The two interval forms answer different
 * questions, and picking the wrong one is the classic scheduling bug.
 *
 * <table border="1">
 * <caption>The difference in one line</caption>
 * <tr><th></th><th>{@code every}</th><th>{@code everyAfterFinish}</th></tr>
 * <tr><td>Anchored to</td><td>the SCHEDULED time</td><td>the END of the previous execution</td></tr>
 * <tr><td>Cadence</td><td>fixed rate</td><td>fixed delay</td></tr>
 * <tr><td>A run that takes longer than the interval</td><td>the next firing is already due</td><td>the gap is always honoured</td></tr>
 * <tr><td>Spring equivalent</td><td>{@code @Scheduled(fixedRate)}</td><td>{@code @Scheduled(fixedDelay)}</td></tr>
 * </table>
 *
 * <p>Both take an <b>ISO-8601 duration</b>: {@code PT30S}, {@code PT5M}, {@code PT1H30M}. The
 * human-looking {@code "30s"} is the mistake people actually make, so the boot fails naming the
 * job, the attribute and the format it wanted, instead of dying on a bare
 * {@code DateTimeParseException}.
 *
 * <h2>Choosing between them</h2>
 *
 * <p>Use {@code every} when the cadence itself is the requirement — a metric sampled every 30
 * seconds is meaningless if the samples drift apart. Use {@code everyAfterFinish} when the
 * requirement is the REST between runs — a poll against a rate-limited third party wants a
 * guaranteed gap, and would otherwise queue up behind itself the moment one call is slow.
 *
 * <p>A fixed-rate job whose duration can exceed its interval also wants
 * {@link ConcurrencyPolicyExample the overlap policy}: fixed rate on its own will happily start the
 * next execution while the previous one is still running.
 */
@Component
public class IntervalJobExample {

    private static final Logger log = LoggerFactory.getLogger(IntervalJobExample.class);

    /**
     * Fixed rate: occurrences land every 30 minutes regardless of how long each one takes. The
     * interval is rearmed from the SCHEDULED time, so a slow run does not push the following ones
     * later — the cadence is preserved, at the cost of possible overlap.
     */
    @RecurringJob(id = "example-metrics-rollup", every = "PT30M")
    void metricsRollup() {
        log.info("rolling up the last half hour");
    }

    /**
     * Fixed delay: the next occurrence is armed inside the completion transaction, {@code PT15M}
     * after this method returns. Two consecutive runs can therefore never overlap, and the observed
     * period is {@code duration + PT15M} rather than {@code PT15M}.
     *
     * <p>The exact same schedule, expressed programmatically, is
     * {@code new IntervalSpec(Duration.ofMinutes(15), true)} — the {@code afterFinish} flag is this
     * distinction (see {@link ProgrammaticDefinitionExample}).
     */
    @RecurringJob(id = "example-partner-poll", everyAfterFinish = "PT15M", retries = 2)
    void partnerPoll() {
        log.info("polling the partner API, then resting {}", Duration.ofMinutes(15));
    }
}
