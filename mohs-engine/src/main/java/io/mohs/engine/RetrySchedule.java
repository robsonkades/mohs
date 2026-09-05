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
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

/**
 * The retry decision shared by {@link Dispatcher} (an attempt failure) and by the reaper (reclaiming
 * an expired lease): one place decides budget and backoff — two failure paths with their own copies
 * of the policy would diverge on the first change (Shotgun Surgery).
 *
 * <p>Exponential backoff with full jitter (the AWS style): a delay uniform in
 * {@code [0, min(1s x 2^(attempt-1), 10min)]}. Full jitter rather than pure exponential because the
 * 3 a.m. case is a shared resource going down and taking many executions with it — without jitter,
 * they would all come back in lockstep against a resource that is still recovering (a thundering
 * herd).
 *
 * <p>Internal constants, with no configuration property: a job that needs different numbers declares
 * a {@link io.mohs.core.execution.RetryPolicy} instead, and {@link RetryPolicyRegistry} is what
 * chooses between the two. This class is the default, and the fallback for a policy that misbehaves.
 */
public final class RetrySchedule {

    private static final Duration BASE = Duration.ofSeconds(1);
    private static final Duration CAP = Duration.ofMinutes(10);
    /** 2^20 x 1s already exceeds the CAP — a larger exponent would only risk overflowing the shift. */
    private static final int MAX_EXPONENT = 20;

    private RetrySchedule() {
    }

    /**
     * The time of the next retry after attempt {@code failedAttempt} (1-based) fails — empty when the
     * budget has run out ({@code retries} being attempts beyond the first, per
     * {@code JobDefinition.retries}'s contract: total attempts = retries + 1).
     *
     * @param failedAttempt the one-based number of the failed attempt
     * @param retries the allowed retry attempts beyond the first
     * @param now the current instant from the configured time source
     * @return the retry instant, or empty when the retry budget is exhausted
     */
    public static Optional<Instant> nextRetryAt(int failedAttempt, int retries, Instant now) {
        if (failedAttempt < 1) {
            throw new IllegalArgumentException("failedAttempt must be at least 1");
        }
        if (retries < 0) {
            throw new IllegalArgumentException("retries must not be negative");
        }
        Objects.requireNonNull(now, "now");
        if (failedAttempt > retries) {
            return Optional.empty();
        }
        long exponent = Math.min(failedAttempt - 1L, MAX_EXPONENT);
        long boundMillis = Math.min(BASE.toMillis() << exponent, CAP.toMillis());
        long delayMillis = ThreadLocalRandom.current().nextLong(boundMillis + 1);
        return Optional.of(now.plus(Duration.ofMillis(delayMillis)));
    }
}
