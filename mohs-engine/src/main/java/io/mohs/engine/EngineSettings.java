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
 * Runtime loop configuration. Session expiry uses nodeLeaseTtl and the database clock.
 * Zero history retention disables history pruning. Idempotency expiry is fixed by acceptance,
 * independently of history. All callers use this single constructor.
 */
public record EngineSettings(Duration pollInterval, Duration maxPollInterval, int batchSize, int dispatchConcurrency,
        int claimRounds, Duration nodeLeaseTtl, @Nullable Duration watchdogTimeout,
        Duration misfireThreshold, Duration historyRetention, Duration startupDelay) {
    public static final Duration DEFAULT_MISFIRE_THRESHOLD = Duration.ofSeconds(60);

    public EngineSettings {
        Objects.requireNonNull(startupDelay, "startupDelay");
        if (startupDelay.isNegative()) {
            throw new IllegalArgumentException("mohs.lifecycle.startup-delay must not be negative");
        }
        try {
            startupDelay.toNanos();
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException("mohs.lifecycle.startup-delay is too large", e);
        }
        Objects.requireNonNull(pollInterval, "pollInterval");
        Objects.requireNonNull(maxPollInterval, "maxPollInterval");
        Objects.requireNonNull(nodeLeaseTtl, "nodeLeaseTtl");
        requireNanoBudget(pollInterval, "poll-interval");
        requireNanoBudget(maxPollInterval, "max-poll-interval");
        requireNanoBudget(nodeLeaseTtl, "node-lease-ttl");
        io.mohs.core.TimePrecision.micros(nodeLeaseTtl);
        Objects.requireNonNull(misfireThreshold, "misfireThreshold");
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
        if (historyRetention.isNegative()) {
            throw new IllegalArgumentException("mohs.engine.history-retention must not be negative, got "
                    + historyRetention + " — zero turns the sweep off and keeps every terminal execution forever, which is as far as it goes");
        }
    }

    private static void requireNanoBudget(Duration value, String field) {
        try {
            value.toNanos();
        } catch (ArithmeticException failure) {
            throw new IllegalArgumentException("mohs.engine." + field + " is too large for a monotonic runtime budget", failure);
        }
    }

}
