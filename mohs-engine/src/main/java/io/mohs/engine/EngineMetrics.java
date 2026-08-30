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
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import io.mohs.core.execution.Attempt;
import io.mohs.core.execution.ExecutionState;
import io.mohs.core.job.JobKey;

/**
 * The engine's {@code mohs.*} metrics — operability is a requirement, not finishing polish (design
 * for 3 a.m.).
 *
 * <p>The cardinality rule is enforced here rather than by convention: the labels are {@code job}
 * (bounded by the number of definitions) and {@code outcome}/{@code reason} (enums) — an execution
 * id NEVER becomes a label.
 *
 * <p>Execution duration derives from the {@link Attempt}'s timestamps — the same persisted window
 * the benchmarks and the dashboard read, so there is a single source. Claim latency arrives in nanoseconds
 * measured by {@code System.nanoTime} in the caller (the project's monotonic-time invariant). During
 * a backwards clock resync the window may come out negative and Micrometer's {@code Timer} silently
 * drops the sample — missing samples during a resync is expected behaviour, not data loss.
 *
 * <p>Label values are contract just as much as the names: lower case, snake_case
 * ({@code succeeded}, {@code attempts_exhausted}) — the first saved dashboard freezes that
 * vocabulary.
 */
public final class EngineMetrics {

    private final MeterRegistry registry;
    private final Timer claimLatency;
    private final DistributionSummary claimBatchSize;

    public EngineMetrics(MeterRegistry registry) {
        this.registry = Objects.requireNonNull(registry, "registry");
        this.claimLatency = Timer.builder("mohs.claim.latency")
                .description("duration of one claim round against the store")
                .register(registry);
        this.claimBatchSize = DistributionSummary.builder("mohs.claim.batch.size")
                .description("executions claimed per round — full batches mean claim-bound, small ones dispatch-bound")
                .register(registry);
        // Pre-registering the known reasons: a lazy counter is only born on its first increment, and
        // an alert using increase() cannot distinguish "series missing" from zero — the series
        // existing from boot is the contract
        for (String reason : new String[] {"concurrency-cap", "rate-limit", "window-closed", "stray-lease"}) {
            registry.counter("mohs.claim.requeued", "reason", reason);
        }
    }

    void claimRound(long elapsedNanos, int claimed) {
        claimLatency.record(elapsedNanos, TimeUnit.NANOSECONDS);
        claimBatchSize.record(claimed);
    }

    /** From scheduled to the handler's start — the SLO visible to the user, per job. */
    void dispatchLatency(JobKey job, Duration sinceScheduled) {
        registry.timer("mohs.dispatch.latency", "job", job.value()).record(sinceScheduled);
    }

    /**
     * One attempt confirmed by the completion CAS. Every attempt counts in
     * {@code mohs.attempt.total}; only a terminal transition counts in {@code mohs.execution.total} —
     * a retry is not yet the execution's outcome, and the attempts-to-executions ratio is the health
     * indicator.
     */
    void attemptFinished(JobKey job, Attempt attempt, ExecutionState newState) {
        String outcome = labelValue(attempt.outcome());
        registry.counter("mohs.attempt.total", "job", job.value(), "outcome", outcome).increment();
        Instant finishedAt = attempt.finishedAt();
        if (finishedAt != null) {
            registry.timer("mohs.execution.duration", "job", job.value(), "outcome", outcome)
                    .record(Duration.between(attempt.startedAt(), finishedAt));
        }
        if (newState != ExecutionState.RETRY_WAITING) {
            registry.counter("mohs.execution.total", "job", job.value(), "outcome", labelValue(newState)).increment();
        }
    }

    /**
     * Any non-zero value here means a dead or stopped node — the label says what the reclaim decided.
     *
     * <p>{@code attemptsExhausted} separates a FAILED from an exhausted budget from a FAILED from a
     * retired job (the Execution alone cannot tell them apart: both end FAILED — what knows the reason
     * is the reaper's decision, and it becomes the label here). Confusing them would send the operator
     * investigating retry budgets during a mass retirement.
     */
    void leaseReclaimed(ExecutionState postReclaimState, boolean attemptsExhausted) {
        String reason = switch (postReclaimState) {
            case RETRY_WAITING -> "retry";
            case FAILED -> attemptsExhausted ? "attempts_exhausted" : "job_retired";
            case CANCELLED -> "cancelled";
            // Unreachable after a reclaim; explicit arms so the compiler flags a new ExecutionState
            case ENQUEUED, RUNNING, SUCCEEDED -> labelValue(postReclaimState);
        };
        registry.counter("mohs.lease.reclaimed", "reason", reason).increment();
    }

    /**
     * Post-claim admission losses — the cap turned mid-round, or the rate limit was taken by another
     * node between the two phases.
     *
     * <p>Expected churn, bounded to one round per guard flip; a sustained INCREASING value means the
     * inadmissible list is arriving late (the queue's head dominated by a stuck job).
     */
    void claimRequeued(String reason, int count) {
        registry.counter("mohs.claim.requeued", "reason", reason).increment(count);
    }

    /**
     * A tick step failed. On a dashboard at 3 a.m. it separates "the node is idle because the queue is
     * empty" from "the node is idle because every tick dies" — a distinction that previously existed
     * only in a line of {@code log.error}. The label is the STEP, not the exception: the operational
     * question is WHICH part of the tick stopped.
     */
    void tickStepFailed(String step) {
        registry.counter("mohs.tick.failed", "step", step).increment();
    }

    private static String labelValue(ExecutionState state) {
        return state.name().toLowerCase(Locale.ROOT);
    }

    /**
     * {@code strongReference}: the supplier is an ephemeral method reference — without it the gauge
     * would vanish at the first GC.
     *
     * <p>A recorded limitation: with no {@code node} label, two engines in the same registry (one Mohs
     * per datasource) collide on the id and the second bind is silently ignored by Micrometer. The
     * trigger for adding the tag is the first real multi-engine scenario, paying the cardinality only
     * then.
     */
    void bindNodeGauges(Supplier<Number> inFlight, int capacity) {
        Gauge.builder("mohs.node.inflight", inFlight)
                .description("executions currently dispatched on this node")
                .strongReference(true)
                .register(registry);
        Gauge.builder("mohs.node.capacity", () -> capacity)
                .description("dispatch-concurrency of this node")
                .strongReference(true)
                .register(registry);
    }

    /**
     * The backlog — the leading indicator of trouble, and the one signal that says "the cluster is
     * falling behind" before any latency percentile moves.
     *
     * <p>The supplier reads a value the engine SAMPLED on its own cadence; it does not query the
     * database. That is deliberate: a gauge is evaluated once per scrape, by every scraper, and a
     * counting query on that path would let a monitoring system decide the load it is measuring.
     *
     * <p>The number is cluster-wide, so every node publishes the same one: aggregate it with
     * {@code max} across instances, never {@code sum}.
     */
    void bindQueueDepthGauge(Supplier<Number> depth) {
        Gauge.builder("mohs.queue.depth", depth)
                .description("entries in the queue visible to a claim — the backlog, cluster-wide")
                .strongReference(true)
                .register(registry);
    }
}
