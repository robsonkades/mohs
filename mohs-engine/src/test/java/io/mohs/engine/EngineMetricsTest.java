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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import io.mohs.core.execution.Attempt;
import io.mohs.core.execution.ExecutionState;
import io.mohs.core.job.JobKey;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link EngineMetrics}'s contract: {@code mohs.*} names, bounded labels ({@code job},
 * {@code outcome}, {@code reason} — never an execution id) and the counting rule that separates an
 * attempt from an execution (a retry counts as an attempt, not as an execution's outcome).
 */
class EngineMetricsTest {

    private static final JobKey JOB = JobKey.of("billing");
    private static final Instant STARTED = Instant.parse("2026-08-21T12:00:00Z");

    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    private final EngineMetrics metrics = new EngineMetrics(registry);

    @Test
    void claimRoundRecordsLatencyAndBatchSize() {
        metrics.claimRound(Duration.ofMillis(7).toNanos(), 250);

        assertThat(registry.get("mohs.claim.latency").timer().count()).isEqualTo(1);
        assertThat(registry.get("mohs.claim.latency").timer().totalTime(TimeUnit.MILLISECONDS)).isEqualTo(7.0);
        assertThat(registry.get("mohs.claim.batch.size").summary().totalAmount()).isEqualTo(250.0);
    }

    @Test
    void dispatchLatencyIsTaggedByJob() {
        metrics.dispatchLatency(JOB, Duration.ofMillis(25));

        assertThat(registry.get("mohs.dispatch.latency").tag("job", "billing").timer().count()).isEqualTo(1);
    }

    @Test
    void terminalAttemptCountsAttemptDurationAndExecutionOutcome() {
        Attempt attempt = new Attempt(1, STARTED, STARTED.plusMillis(120), ExecutionState.SUCCEEDED, null);

        metrics.attemptFinished(JOB, attempt, ExecutionState.SUCCEEDED);

        assertThat(registry.get("mohs.attempt.total").tag("job", "billing").tag("outcome", "succeeded")
                .counter().count()).isEqualTo(1.0);
        assertThat(registry.get("mohs.execution.duration").tag("job", "billing").tag("outcome", "succeeded")
                .timer().totalTime(TimeUnit.MILLISECONDS)).isEqualTo(120.0);
        assertThat(registry.get("mohs.execution.total").tag("job", "billing").tag("outcome", "succeeded")
                .counter().count()).isEqualTo(1.0);
    }

    @Test
    void retryCountsTheAttemptButNotAnExecutionOutcome() {
        Attempt attempt = new Attempt(1, STARTED, STARTED.plusMillis(30), ExecutionState.FAILED, "boom");

        metrics.attemptFinished(JOB, attempt, ExecutionState.RETRY_WAITING);

        assertThat(registry.get("mohs.attempt.total").tag("outcome", "failed").counter().count()).isEqualTo(1.0);
        assertThat(registry.get("mohs.execution.duration").tag("outcome", "failed").timer().count()).isEqualTo(1);
        assertThat(registry.find("mohs.execution.total").counter()).isNull();
    }

    @Test
    void leaseReclaimedSeparatesExhaustedBudgetFromRetiredJob() {
        metrics.leaseReclaimed(ExecutionState.RETRY_WAITING, false);
        metrics.leaseReclaimed(ExecutionState.FAILED, true);
        metrics.leaseReclaimed(ExecutionState.FAILED, false);
        metrics.leaseReclaimed(ExecutionState.CANCELLED, false);

        assertThat(registry.get("mohs.lease.reclaimed").tag("reason", "retry").counter().count()).isEqualTo(1.0);
        assertThat(registry.get("mohs.lease.reclaimed").tag("reason", "attempts_exhausted").counter().count()).isEqualTo(1.0);
        assertThat(registry.get("mohs.lease.reclaimed").tag("reason", "job_retired").counter().count()).isEqualTo(1.0);
        assertThat(registry.get("mohs.lease.reclaimed").tag("reason", "cancelled").counter().count()).isEqualTo(1.0);
    }

    @Test
    void nodeGaugesTrackInFlightAndCapacity() {
        AtomicInteger inFlight = new AtomicInteger(3);

        metrics.bindNodeGauges(inFlight::get, 1024);

        assertThat(registry.get("mohs.node.inflight").gauge().value()).isEqualTo(3.0);
        inFlight.set(7);
        assertThat(registry.get("mohs.node.inflight").gauge().value()).isEqualTo(7.0);
        assertThat(registry.get("mohs.node.capacity").gauge().value()).isEqualTo(1024.0);
    }
}
