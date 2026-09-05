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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import io.mohs.core.definition.OnDemandJob;
import io.mohs.core.definition.RecurringJob;

/**
 * <b>Scenario 10 — how many executions of the SAME job may run at once.</b>
 *
 * <p>The default is unbounded, and that default is right far more often than it looks. Most jobs are
 * invoked many times with independent payloads — one execution per recipient, per row, per invoice —
 * and those have no reason to serialise against each other merely for sharing a {@code job_key}.
 * Serialising them by default would turn a scheduler into a queue with one consumer.
 *
 * <p>The narrow case where a cap is right is the recurring job whose own next firing can arrive
 * before the previous one finished: a synchronisation that usually takes two minutes on a
 * five-minute interval, and occasionally takes seven. There the two "executions" are the same task
 * overlapping itself, not independent work. Quartz's {@code @DisallowConcurrentExecution} is opt-in
 * for the same reason.
 *
 * <p>The cap is cluster-wide and derived from live leases, not from a local counter — which is what
 * makes it hold when the job is running on three nodes at once.
 *
 * <h2>The two knobs</h2>
 *
 * <table border="1">
 * <caption>{@code allowConcurrentExecutions} and {@code maxConcurrentExecutions}</caption>
 * <tr><th>Declaration</th><th>Meaning</th></tr>
 * <tr><td>(nothing)</td><td>no ceiling — the default</td></tr>
 * <tr><td>{@code allowConcurrentExecutions = false}, {@code maxConcurrentExecutions = 1}</td><td>strict mutual exclusion</td></tr>
 * <tr><td>{@code allowConcurrentExecutions = false}, {@code maxConcurrentExecutions = N}</td><td>at most N at a time</td></tr>
 * <tr><td>{@code allowConcurrentExecutions = false} alone</td><td><b>boot error</b> — the default 0 is not assumed to mean 1</td></tr>
 * </table>
 *
 * <p>Programmatically the same two are {@code spec.preventOverlap()} and
 * {@code spec.maxConcurrentExecutions(n)} — see {@link ProgrammaticDefinitionExample}.
 *
 * <p>A capped job is not a lost job: what exceeds the ceiling stays enqueued and is claimed as soon
 * as a slot frees. The cap shapes concurrency, it does not discard work.
 */
@Component
public class ConcurrencyPolicyExample {

    /**
     * Creates a {@code ConcurrencyPolicyExample} instance.
     */
    public ConcurrencyPolicyExample() {
    }

    private static final Logger log = LoggerFactory.getLogger(ConcurrencyPolicyExample.class);

    /**
     * Strict: never two of these at once, anywhere in the cluster. The pairing of a fixed-rate
     * interval with a job that can outlast its own interval is exactly what this is for.
     */
    @RecurringJob(
            id = "example-catalog-sync",
            every = "PT5M",
            allowConcurrentExecutions = false,
            maxConcurrentExecutions = 1
    )
    void catalogSync() {
        log.info("synchronising the catalogue — alone");
    }

    /**
     * A ceiling above one: the handler shares an external resource that tolerates three concurrent
     * users of it and no more. Different from a {@link RateLimitExample rate limit}, which bounds
     * how OFTEN work starts rather than how much is in flight, and different from a
     * {@link RunnerExample runner}, whose cap is per node and shared across jobs.
     */
    @OnDemandJob(id = "example-legacy-export", allowConcurrentExecutions = false, maxConcurrentExecutions = 3)
    void legacyExport() {
        log.info("one of at most three");
    }
}
