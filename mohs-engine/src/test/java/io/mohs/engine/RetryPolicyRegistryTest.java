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
import java.util.Optional;

import org.junit.jupiter.api.Test;

import io.mohs.core.definition.JobDefinition;
import io.mohs.core.execution.RetryPolicy;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The decision point both failure paths share — {@link Dispatcher}'s throw and the reaper's dead node. */
class RetryPolicyRegistryTest {

    private static final Instant NOW = Instant.parse("2026-08-29T12:00:00Z");

    record Handler() {
    }

    private static JobDefinition jobWithoutPolicy(int retries) {
        return JobDefinition.of("invoices", Handler.class, spec -> spec.onDemand().retries(retries));
    }

    private static JobDefinition job(int retries, String policy) {
        return JobDefinition.of("invoices", Handler.class,
                spec -> spec.onDemand().retries(retries).retryPolicy(policy));
    }

    /** No policy named: the built-in schedule decides, and its budget still governs. */
    @Test
    void withoutAPolicyTheBudgetDecides() {
        RetryPolicyRegistry registry = RetryPolicyRegistry.empty();

        assertThat(registry.nextRetryAt(jobWithoutPolicy(1), 1, new IllegalStateException("boom"), NOW)).isPresent();
        assertThat(registry.nextRetryAt(jobWithoutPolicy(1), 2, new IllegalStateException("boom"), NOW)).isEmpty();
    }

    /** The policy REPLACES the budget — that is the reason to write one, and this job's budget is zero. */
    @Test
    void aPolicyGrantsRetriesTheDeclaredBudgetWouldRefuse() {
        RetryPolicyRegistry registry = new RetryPolicyRegistry(
                Map.of("fixed", _ -> Optional.of(Duration.ofSeconds(30))));

        assertThat(registry.nextRetryAt(job(0, "fixed"), 7, null, NOW)).contains(NOW.plusSeconds(30));
    }

    /** Empty is terminal, whatever the budget says. */
    @Test
    void anEmptyDecisionIsTerminalEvenWithBudgetLeft() {
        RetryPolicyRegistry registry = new RetryPolicyRegistry(Map.of("giveUp", _ -> Optional.empty()));

        assertThat(registry.nextRetryAt(job(10, "giveUp"), 1, null, NOW)).isEmpty();
    }

    /** The reaper's call: the node died, so there is no Throwable to hand over — the policy must still be able to decide. */
    @Test
    void aDeadNodeReachesThePolicyWithNoError() {
        RetryPolicyRegistry registry = new RetryPolicyRegistry(Map.of("inspect",
                failure -> failure.error() == null ? Optional.of(Duration.ofMinutes(1)) : Optional.empty()));

        assertThat(registry.nextRetryAt(job(0, "inspect"), 1, null, NOW)).contains(NOW.plusSeconds(60));
    }

    /**
     * A bug in someone's Strategy must not strand the execution in {@code RUNNING}: the completion
     * still happens, on the built-in schedule.
     */
    @Test
    void aThrowingPolicyFallsBackToTheBuiltInSchedule() {
        RetryPolicyRegistry registry = new RetryPolicyRegistry(Map.of("broken", _ -> {
            throw new IllegalStateException("policy bug");
        }));

        assertThat(registry.nextRetryAt(job(1, "broken"), 1, null, NOW)).isPresent();
        assertThat(registry.nextRetryAt(job(0, "broken"), 1, null, NOW))
                .as("the fallback is the built-in schedule, budget included").isEmpty();
    }

    /** A negative delay is a policy bug like any other, and takes the same path. */
    @Test
    void aNegativeDelayFallsBackToTheBuiltInSchedule() {
        RetryPolicyRegistry registry = new RetryPolicyRegistry(
                Map.of("backwards", _ -> Optional.of(Duration.ofSeconds(-1))));

        assertThat(registry.nextRetryAt(job(0, "backwards"), 1, null, NOW)).isEmpty();
    }

    /**
     * The runtime counterpart of the boot check: a definition naming a policy no bean on THIS node
     * declares is a rollout out of step, not a policy bug, and the failure still gets its retry.
     * Stranding it would be the one outcome the completion path may not produce.
     */
    @Test
    void aPolicyNameThisNodeDoesNotDeclareFallsBackToTheBuiltInSchedule() {
        RetryPolicyRegistry registry = new RetryPolicyRegistry(
                Map.of("known", _ -> Optional.of(Duration.ofSeconds(1))));

        assertThat(registry.nextRetryAt(job(1, "ghost"), 1, null, NOW))
                .as("the retry still happens, on the built-in schedule").isPresent();
        assertThat(registry.nextRetryAt(job(0, "ghost"), 1, null, NOW))
                .as("and the built-in budget still decides when to stop").isEmpty();
    }

    /** The message has to name what to do — this is what a boot failure shows the operator. */
    @Test
    void anUnknownPolicyNameIsRejectedNamingTheDeclaredOnes() {
        RetryPolicyRegistry registry = new RetryPolicyRegistry(
                Map.of("known", _ -> Optional.of(Duration.ofSeconds(1))));

        assertThat(registry.contains("known")).isTrue();
        assertThat(registry.contains("ghost")).isFalse();
        assertThatThrownBy(() -> registry.require("ghost"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("ghost")
                .hasMessageContaining("known");
    }
}
