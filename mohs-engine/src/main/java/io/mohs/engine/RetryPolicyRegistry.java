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
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mohs.core.definition.JobDefinition;
import io.mohs.core.execution.RetryPolicy;

/**
 * The single place that answers "is there another attempt, and when" — the built-in
 * {@link RetrySchedule} unless the definition names a {@link RetryPolicy}.
 *
 * <p>It exists as one collaborator rather than a lookup at each call site for the reason
 * {@link RetrySchedule}'s Javadoc gives about itself: the two failure paths — a handler that threw
 * ({@link Dispatcher}) and a lease reclaimed from a dead node (the reaper) — must not each grow
 * their own copy of the decision (Shotgun Surgery).
 *
 * <p>Resolution is by name, and the boot check ({@link #require}) refuses an unknown one: by the time
 * an execution fails, silently applying the default backoff would be indistinguishable from the
 * custom policy having decided it. A name that still reaches a FAILURE unresolved is therefore not a
 * typo but a node out of step with the definition — an orphaned job, or a rollout in progress — and
 * it completes on the built-in schedule with a log that says which of the two it is.
 */
public final class RetryPolicyRegistry {

    private static final Logger log = LoggerFactory.getLogger(RetryPolicyRegistry.class);

    private final Map<String, RetryPolicy> policies;

    /**
     * Creates a {@code RetryPolicyRegistry} with the supplied values.
     *
     * @param policies the named retry policies
     */
    public RetryPolicyRegistry(Map<String, RetryPolicy> policies) {
        this.policies = Map.copyOf(Objects.requireNonNull(policies, "policies"));
    }

    /**
     * The registry a deployment with no custom policy gets — every job on the built-in backoff.
     *
     * @return a registry with no custom policies
     */
    public static RetryPolicyRegistry empty() {
        return new RetryPolicyRegistry(Map.of());
    }

    /**
     * Fails when the name has no policy, so the gap surfaces at boot rather than at the first
     * failure of the job that declared it.
     *
     * @param name the registered retry-policy name
     * @return the registered policy
     */
    public RetryPolicy require(String name) {
        RetryPolicy policy = policies.get(name);
        if (policy == null) {
            throw new IllegalStateException("no RetryPolicy bean named '" + name + "' — declare one, or drop the "
                    + "retryPolicy attribute to use the built-in exponential backoff with full jitter " + declared());
        }
        return policy;
    }

    /** The tail of the message above: what IS declared is the operator's shortest path to the typo. */
    private String declared() {
        return policies.isEmpty() ? "(no RetryPolicy bean is declared at all)" : "(declared: " + policies.keySet() + ")";
    }

    /**
     * Checks whether a named retry policy is registered.
     *
     * @param name the registered retry-policy name
     * @return whether the name is registered
     */
    public boolean contains(String name) {
        return policies.containsKey(name);
    }

    /**
     * When the next attempt runs, or empty when this failure is terminal.
     *
     * <p>A custom policy's exception is not allowed to swallow the failure it was deciding about:
     * the execution falls back to the built-in schedule and the policy's throw is logged. Anything
     * else would let a bug in a user's Strategy strand executions in {@code RUNNING} — the one
     * outcome the completion path must never produce.
     *
     * @param definition the registered job definition
     * @param failedAttempt the one-based number of the failed attempt
     * @param error the failure that ended the attempt
     * @param now the current instant from the configured time source
     * @return the retry instant, or empty when the failure is terminal
     */
    public Optional<Instant> nextRetryAt(JobDefinition definition, int failedAttempt, @Nullable Throwable error,
            Instant now) {
        Objects.requireNonNull(definition, "definition");
        Objects.requireNonNull(now, "now");
        String name = definition.retryPolicy();
        if (name == null) {
            return builtInBackoff(definition, failedAttempt, now);
        }
        RetryPolicy policy = policies.get(name);
        if (policy == null) {
            // Not a policy bug, and it must not be reported as one: the boot check clears every live
            // definition, so reaching here means the definition came from a node this one does not
            // match — an orphaned job whose bean legitimately went away, or a cluster mid-rollout
            log.error("job '{}' names RetryPolicy '{}', which no bean on THIS node declares {} — using the built-in "
                    + "backoff for this failure; check for a mixed-version rollout",
                    definition.key().value(), name, declared());
            return builtInBackoff(definition, failedAttempt, now);
        }
        try {
            RetryPolicy.Failure failure =
                    new RetryPolicy.Failure(definition.key(), failedAttempt, definition.retries(), error);
            return policy.nextDelay(failure).map(delay -> now.plus(requireNonNegative(delay, name)));
        } catch (RuntimeException | LinkageError | StackOverflowError | AssertionError e) {
            // Wider than RuntimeException on purpose: at an SPI boundary the unit of failure is the
            // call into someone else's code, not the exception's type. A policy whose class fails to
            // initialise throws an Error, and letting that escape would leave the execution RUNNING
            // with its lease held by a node that is alive — the one outcome nothing here may produce
            log.error("RetryPolicy '{}' declared by job '{}' failed on attempt {} — falling back to the built-in "
                    + "backoff for this failure", name, definition.key().value(), failedAttempt, e);
            return builtInBackoff(definition, failedAttempt, now);
        }
    }

    private static Optional<Instant> builtInBackoff(JobDefinition definition, int failedAttempt, Instant now) {
        return RetrySchedule.nextRetryAt(failedAttempt, definition.retries(), now);
    }

    /** A delay pointing into the past is a policy bug like any other, and takes the same fallback path. */
    private static Duration requireNonNegative(Duration delay, String name) {
        if (delay.isNegative()) {
            throw new IllegalStateException("RetryPolicy '" + name + "' returned a negative delay " + delay);
        }
        return delay;
    }
}
