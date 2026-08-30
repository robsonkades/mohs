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
package io.mohs.core.execution;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

import org.jspecify.annotations.Nullable;

import io.mohs.core.job.JobKey;

/**
 * A per-job retry decision, for the cases {@code retries} cannot express — a Strategy (GoF) over the
 * built-in exponential backoff with full jitter.
 *
 * <p>Declared by BEAN NAME ({@code @MohsJob(retryPolicy = "…")}) rather than by type, because the
 * definition is persisted: a name survives a restart and a class name would tie stored rows to a
 * class that may be renamed. A job that names a policy with no matching bean fails the boot rather
 * than silently falling back.
 *
 * <p>The policy REPLACES the budget: while it returns a delay the execution retries, and
 * {@link Failure#retries()} is information rather than a ceiling. A policy that never returns empty
 * therefore never gives up — which is the point of writing one, and its risk.
 *
 * <p>It is consulted on both failure paths: a handler that threw (with the exception in
 * {@link Failure#error()}) and a lease reclaimed from a node that died (no exception — the failure
 * happened somewhere the JVM is no longer running). On the first it runs on the completion path,
 * holding the execution's transaction open. On the second it runs on the ENGINE'S LOOP THREAD, once
 * per reclaimed lease — and that thread also carries the node's heartbeat, so a policy that blocks
 * there delays the heartbeat, and a node that misses its heartbeat has its work reclaimed by a peer.
 *
 * <p>It must therefore be side-effect free, do no I/O, and return in microseconds. It is a decision,
 * not a lookup: everything it needs to decide with is in {@link Failure}.
 *
 * <p>Example — give up quickly on a bad request, keep trying on a timeout:
 *
 * {@snippet :
 * @Bean("invoiceRetries")
 * RetryPolicy invoiceRetries() {
 *     return failure -> switch (failure.error()) {
 *         case IllegalArgumentException _ -> Optional.empty();
 *         case null -> Optional.of(Duration.ofSeconds(30));
 *         default -> failure.failedAttempt() <= 10 ? Optional.of(Duration.ofMinutes(1)) : Optional.empty();
 *     };
 * }
 * }
 */
@FunctionalInterface
public interface RetryPolicy {

    /**
     * How long to wait before the next attempt, or {@link Optional#empty()} to fail terminally.
     *
     * <p>A negative delay is rejected as a programming error, and a zero delay is honoured — it
     * means "as soon as a claim can take it", not "immediately on this thread".
     */
    Optional<Duration> nextDelay(Failure failure);

    /**
     * The failure being decided on.
     *
     * @param jobKey the job whose execution failed
     * @param failedAttempt which attempt just failed, 1-based
     * @param retries the budget the definition declares — informative here, since the policy is what decides
     * @param error what the handler threw, or {@code null} when the failure is a dead node's reclaimed lease
     */
    record Failure(JobKey jobKey, int failedAttempt, int retries, @Nullable Throwable error) {

        public Failure {
            Objects.requireNonNull(jobKey, "jobKey");
            if (failedAttempt < 1) {
                throw new IllegalArgumentException("failedAttempt must be at least 1, got " + failedAttempt);
            }
            if (retries < 0) {
                throw new IllegalArgumentException("retries must not be negative, got " + retries);
            }
        }
    }
}
