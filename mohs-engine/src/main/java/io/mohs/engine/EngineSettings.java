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
import java.util.Objects;

import org.jspecify.annotations.Nullable;

/**
 * The {@link Engine}'s timing and batching parameters — an immutable snapshot in place of positional
 * constructor parameters that only kept growing (a Long Parameter List).
 *
 * <p>{@code leaseTtl} is the same value that feeds the claim ({@code mohs.engine.lease-ttl}): the
 * horizon an EXECUTION's lease is born with at claim time — and also the staleness cutoff for a
 * legacy node row with no {@code expires_at}.
 *
 * <p>{@code nodeLeaseTtl} ({@code mohs.engine.node-lease-ttl}) is the NODE's lease: each tick's
 * heartbeat promises "I am alive until now+TTL" in {@code mohs_nodes.expires_at}, and the reaper
 * only considers dead a node whose promise has expired — the liveness authority that replaced
 * per-execution renewal.
 *
 * <p>{@code watchdogTimeout} is the optional runtime ceiling (the Watchdog Bound): once exceeded,
 * the node RELEASES the execution's ownership rather than merely stopping renewal. {@code null}
 * means no ceiling; when present it must be greater than the node's lease — a smaller bound would
 * release ownership before the node could even be considered dead. The bound measures submit-to-now
 * in monotonic time: waiting in a CPU runner's queue counts as runtime — deliberate semantics until
 * the job timeout's interrupt brings a stamp of the real start.
 *
 * <p>{@code misfireThreshold} separates a late firing from a missed one: an occurrence due within
 * the threshold fires late under any policy; one older than it answers to the job's
 * {@code Misfire}.
 *
 * <p>{@code idempotencyRetention} IS the deduplication window: a key deduplicates for as long as its
 * row survives in {@code mohs_idempotency}, so the retention is not housekeeping bolted onto the
 * contract — it is the contract's duration. {@code ZERO} keeps every key forever, which also means
 * the table grows forever.
 *
 * <p>{@code historyRetention} is how long a TERMINAL execution's history (its row, its attempts, and
 * a batch none of whose members remain) survives after finishing. {@code ZERO} — the default — keeps
 * everything forever: deleting history is the operator's call, never a scheduler's surprise, so the
 * sweep is strictly opt-in. It is retention of history, not of the dedup contract:
 * {@code mohs_idempotency} answers only to {@code idempotencyRetention}, so a key may deduplicate
 * against an execution whose history is already gone — that is correct, not a leak.
 *
 * <p>{@code maxPollInterval} is the adaptive backoff's ceiling: the interval between ticks starts at
 * {@code pollInterval} (the floor), doubles on every tick that found no work, and returns to the
 * floor on the first that did. {@code maxPollInterval == pollInterval} disables the backoff (a fixed
 * cadence — the earlier shape, which the convenience constructors preserve for deterministic
 * testing). Each round's actual sleep is further bounded by {@code nodeLeaseTtl/3}: the heartbeat has
 * its own cadence, and a backoff ceiling larger than the liveness promise must NOT let an idle node
 * be declared dead.
 *
 * <p>{@code dispatchConcurrency} is the node's ceiling on in-flight executions — the same value that
 * sizes the built-in {@code io} runner ({@code mohs.engine.dispatch-concurrency}); each tick's claim
 * is bounded by the headroom against it, so the node never claims what it cannot dispatch.
 *
 * <p>{@code claimRounds} is how many claims ONE tick may chain while the batch keeps coming back full
 * and dispatch has headroom — it loosens the coupling between throughput and {@code poll-interval}
 * under backlog (the per-cycle ceiling remains the dispatch headroom); {@code 1} (the default)
 * preserves the classic one-claim-per-tick shape. A batch that comes back smaller than requested ends
 * the rounds (the queue drained — the next round would be an empty SELECT).
 *
 * <p>Sizing: the tick emits its heartbeat ONCE, before the rounds — which is why the rounds carry a
 * monotonic budget of {@code nodeLeaseTtl/4} on top of the counter. Without it,
 * {@code claimRounds x claim-latency} close to the TTL would let a long tick expire the node's lease
 * mid-round and have another node's reaper duplicate everything in flight. A longer tick also delays
 * the timeout and cancel signals — one more reason for rounds to be few.
 *
 * @param pollInterval the minimum interval between polling ticks
 * @param maxPollInterval the ceiling of the idle polling backoff
 * @param batchSize the maximum number of entries in one batch
 * @param dispatchConcurrency the maximum in-flight executions on this node
 * @param claimRounds the maximum claim rounds in one polling tick
 * @param leaseTtl the execution lease duration and legacy-node staleness threshold
 * @param nodeLeaseTtl the duration of the node liveness promise
 * @param watchdogTimeout the optional runtime ceiling; {@code null} disables it
 * @param misfireThreshold the lateness beyond which the misfire policy applies
 * @param idempotencyRetention the deduplication retention window; zero retains keys indefinitely
 * @param historyRetention the terminal-history retention window; zero disables pruning
 */
public record EngineSettings(Duration pollInterval, Duration maxPollInterval, int batchSize, int dispatchConcurrency,
        int claimRounds, Duration leaseTtl, Duration nodeLeaseTtl, @Nullable Duration watchdogTimeout,
        Duration misfireThreshold, Duration idempotencyRetention, Duration historyRetention) {

    /**
     * The same default as {@code mohs.engine.idempotency-retention} ({@code MohsProperties}). Seven
     * days covers a weekly job retrying its way through a bad week; {@code ZERO} turns pruning off.
     */
    public static final Duration DEFAULT_IDEMPOTENCY_RETENTION = Duration.ofDays(7);

    /** The same default as {@code mohs.engine.misfire-threshold} ({@code MohsProperties}) — the Quartz precedent. */
    public static final Duration DEFAULT_MISFIRE_THRESHOLD = Duration.ofSeconds(60);

    /**
     * A claim with no dispatch ceiling — the earlier behaviour, preserved by the convenience
     * constructors for test use; production ({@code MohsAutoConfiguration}) always passes the real
     * ceiling through the canonical constructor.
     */
    private static final int UNBOUNDED_DISPATCH = Integer.MAX_VALUE;

    /**
     * Creates a {@code EngineSettings} with the supplied values.
     *
     * @param pollInterval the minimum interval between polling ticks
     * @param maxPollInterval the ceiling of the idle polling backoff
     * @param batchSize the maximum number of entries in one batch
     * @param dispatchConcurrency the maximum in-flight executions on this node
     * @param claimRounds the maximum claim rounds in one polling tick
     * @param leaseTtl the execution lease duration and legacy-node staleness threshold
     * @param nodeLeaseTtl the duration of the node liveness promise
     * @param watchdogTimeout the optional runtime ceiling; {@code null} disables it
     * @param misfireThreshold the lateness beyond which the misfire policy applies
     * @param idempotencyRetention the deduplication retention window; zero retains keys indefinitely
     * @param historyRetention the terminal-history retention window; zero disables pruning
     */
    public EngineSettings {
        Objects.requireNonNull(pollInterval, "pollInterval");
        Objects.requireNonNull(maxPollInterval, "maxPollInterval");
        Objects.requireNonNull(leaseTtl, "leaseTtl");
        Objects.requireNonNull(nodeLeaseTtl, "nodeLeaseTtl");
        Objects.requireNonNull(misfireThreshold, "misfireThreshold");
        Objects.requireNonNull(idempotencyRetention, "idempotencyRetention");
        Objects.requireNonNull(historyRetention, "historyRetention");
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be positive");
        }
        if (dispatchConcurrency <= 0) {
            throw new IllegalArgumentException("mohs.engine.dispatch-concurrency must be positive, got " + dispatchConcurrency);
        }
        if (claimRounds <= 0) {
            throw new IllegalArgumentException("mohs.engine.claim-rounds must be positive, got " + claimRounds
                    + " — 1 is the classic one-claim-per-tick shape, not zero");
        }
        if (!pollInterval.isPositive()) {
            throw new IllegalArgumentException("mohs.engine.poll-interval must be positive, got " + pollInterval);
        }
        if (maxPollInterval.compareTo(pollInterval) < 0) {
            throw new IllegalArgumentException("mohs.engine.max-poll-interval (" + maxPollInterval
                    + ") must be >= mohs.engine.poll-interval (" + pollInterval
                    + ") — it is the ceiling the idle backoff climbs to, not a second floor");
        }
        if (!leaseTtl.isPositive()) {
            throw new IllegalArgumentException("mohs.engine.lease-ttl must be positive, got " + leaseTtl
                    + " — a non-positive lease is born expired and turns the first tick into a reclaim storm");
        }
        if (!nodeLeaseTtl.isPositive()) {
            throw new IllegalArgumentException("mohs.engine.node-lease-ttl must be positive, got " + nodeLeaseTtl
                    + " — a non-positive node lease is born expired and every peer's reaper reclaims this node's work");
        }
        if (watchdogTimeout != null && watchdogTimeout.compareTo(nodeLeaseTtl) <= 0) {
            throw new IllegalArgumentException("mohs.engine.watchdog-timeout (" + watchdogTimeout
                    + ") must be greater than mohs.engine.node-lease-ttl (" + nodeLeaseTtl
                    + ") — the bound is the ceiling ON TOP of node liveness, not a shorter lease");
        }
        if (!misfireThreshold.isPositive()) {
            throw new IllegalArgumentException("mohs.engine.misfire-threshold must be positive, got " + misfireThreshold
                    + " — a non-positive threshold turns every normally-late fire into a misfire");
        }
        if (idempotencyRetention.isNegative()) {
            throw new IllegalArgumentException("mohs.engine.idempotency-retention must not be negative, got "
                    + idempotencyRetention + " — zero turns pruning off and keeps every key forever, which is as far as it goes");
        }
        if (historyRetention.isNegative()) {
            throw new IllegalArgumentException("mohs.engine.history-retention must not be negative, got "
                    + historyRetention + " — zero turns the sweep off and keeps every terminal execution forever, which is as far as it goes");
        }
    }

    /**
     * Every parameter but the history sweep, which stays off — history retention is opt-in
     * (an embedded scheduler must not delete history nobody asked it to), so only the caller that
     * binds the operator's property passes it explicitly.
     *
     * @param pollInterval the minimum interval between polling ticks
     * @param maxPollInterval the ceiling of the idle polling backoff
     * @param batchSize the maximum number of entries in one batch
     * @param dispatchConcurrency the maximum in-flight executions on this node
     * @param claimRounds the maximum claim rounds in one polling tick
     * @param leaseTtl the execution lease duration and legacy-node staleness threshold
     * @param nodeLeaseTtl the duration of the node liveness promise
     * @param watchdogTimeout the optional runtime ceiling; {@code null} disables it
     * @param misfireThreshold the lateness beyond which the misfire policy applies
     * @param idempotencyRetention the deduplication retention window; zero retains keys indefinitely
     */
    public EngineSettings(Duration pollInterval, Duration maxPollInterval, int batchSize, int dispatchConcurrency,
            int claimRounds, Duration leaseTtl, Duration nodeLeaseTtl, @Nullable Duration watchdogTimeout,
            Duration misfireThreshold, Duration idempotencyRetention) {
        this(pollInterval, maxPollInterval, batchSize, dispatchConcurrency, claimRounds, leaseTtl, nodeLeaseTtl,
                watchdogTimeout, misfireThreshold, idempotencyRetention, Duration.ZERO);
    }

    /**
     * Every parameter but the deduplication window, which takes its default — the convenience for callers that do not exercise pruning.
     *
     * @param pollInterval the minimum interval between polling ticks
     * @param maxPollInterval the ceiling of the idle polling backoff
     * @param batchSize the maximum number of entries in one batch
     * @param dispatchConcurrency the maximum in-flight executions on this node
     * @param claimRounds the maximum claim rounds in one polling tick
     * @param leaseTtl the execution lease duration and legacy-node staleness threshold
     * @param nodeLeaseTtl the duration of the node liveness promise
     * @param watchdogTimeout the optional runtime ceiling; {@code null} disables it
     * @param misfireThreshold the lateness beyond which the misfire policy applies
     */
    public EngineSettings(Duration pollInterval, Duration maxPollInterval, int batchSize, int dispatchConcurrency,
            int claimRounds, Duration leaseTtl, Duration nodeLeaseTtl, @Nullable Duration watchdogTimeout,
            Duration misfireThreshold) {
        this(pollInterval, maxPollInterval, batchSize, dispatchConcurrency, claimRounds, leaseTtl, nodeLeaseTtl,
                watchdogTimeout, misfireThreshold, DEFAULT_IDEMPOTENCY_RETENTION);
    }

    /**
     * One claim per tick and a node lease equal to the execution lease — a convenience for callers that only configure the dispatch ceiling.
     *
     * @param pollInterval the minimum interval between polling ticks
     * @param batchSize the maximum number of entries in one batch
     * @param dispatchConcurrency the maximum in-flight executions on this node
     * @param leaseTtl the execution lease duration and legacy-node staleness threshold
     * @param watchdogTimeout the optional runtime ceiling; {@code null} disables it
     * @param misfireThreshold the lateness beyond which the misfire policy applies
     */
    public EngineSettings(Duration pollInterval, int batchSize, int dispatchConcurrency, Duration leaseTtl,
            @Nullable Duration watchdogTimeout, Duration misfireThreshold) {
        this(pollInterval, pollInterval, batchSize, dispatchConcurrency, 1, leaseTtl, leaseTtl, watchdogTimeout,
                misfireThreshold, DEFAULT_IDEMPOTENCY_RETENTION);
    }

    /**
     * The default misfire threshold, a claim with no dispatch ceiling and one claim per tick — a test convenience.
     *
     * @param pollInterval the minimum interval between polling ticks
     * @param batchSize the maximum number of entries in one batch
     * @param leaseTtl the execution lease duration and legacy-node staleness threshold
     * @param watchdogTimeout the optional runtime ceiling; {@code null} disables it
     */
    public EngineSettings(Duration pollInterval, int batchSize, Duration leaseTtl, @Nullable Duration watchdogTimeout) {
        this(pollInterval, batchSize, UNBOUNDED_DISPATCH, leaseTtl, watchdogTimeout, DEFAULT_MISFIRE_THRESHOLD);
    }

    /**
     * No Watchdog Bound, the default misfire threshold, a claim with no dispatch ceiling and one claim per tick — a test convenience.
     *
     * @param pollInterval the minimum interval between polling ticks
     * @param batchSize the maximum number of entries in one batch
     * @param leaseTtl the execution lease duration and legacy-node staleness threshold
     */
    public EngineSettings(Duration pollInterval, int batchSize, Duration leaseTtl) {
        this(pollInterval, batchSize, leaseTtl, null);
    }
}
