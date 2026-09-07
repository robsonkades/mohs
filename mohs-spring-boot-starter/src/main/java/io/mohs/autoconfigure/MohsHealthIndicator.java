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
package io.mohs.autoconfigure;

import java.util.Objects;

import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.boot.health.contributor.Status;

import io.mohs.core.EngineState;
import io.mohs.core.MohsLifecycle;

/**
 * Reports this node's engine state under the {@code mohs} key of {@code /actuator/health}.
 *
 * <p><strong>It never touches the database.</strong> That is the whole design: a health indicator
 * that probes the store turns a database outage into a rolling restart of every healthy pod, which
 * is the one failure this signal must not manufacture. {@link MohsLifecycle#state()} reads a field
 * of the engine on this node, so the answer costs nothing and cannot block.
 *
 * <p>The mapping:
 *
 * <table>
 *   <caption>Engine state to health status</caption>
 *   <tr><th>State</th><th>Status</th><th>Why</th></tr>
 *   <tr><td>{@code RUNNING}</td><td>{@code UP}</td><td>Claiming and heartbeating</td></tr>
 *   <tr><td>{@code STARTING}</td><td>{@code OUT_OF_SERVICE}</td><td>Waiting for the startup delay, no heartbeat yet</td></tr>
 *   <tr><td>{@code PAUSED}, {@code DRAINING}</td><td>{@code OUT_OF_SERVICE}</td>
 *       <td>Alive and heartbeating, deliberately not claiming — an operator's decision or a
 *           shutdown in progress, neither of which is a fault</td></tr>
 *   <tr><td>{@code CREATED}, {@code STOPPED}</td><td>{@code DOWN}</td>
 *       <td>No loop is running: work assigned to this node will not move</td></tr>
 * </table>
 *
 * <p>{@code OUT_OF_SERVICE} rather than a custom {@code DEGRADED}: Boot already maps it to 503 and
 * already orders it below {@code UP}, so a readiness group containing this indicator behaves
 * correctly with no extra configuration — a status Boot does not know would answer 200 and quietly
 * keep a paused node in the load balancer.
 *
 * <p>Which probe should contain it is the host's decision, and the answer is usually <em>not
 * liveness</em>: a {@code DRAINING} node reporting {@code OUT_OF_SERVICE} is doing exactly what it
 * was told to do, and restarting it would abort the drain it is in the middle of.
 */
public class MohsHealthIndicator implements HealthIndicator {

    private final MohsLifecycle lifecycle;

    /**
     * Creates a {@code MohsHealthIndicator} with the supplied values.
     *
     * @param lifecycle the local engine lifecycle controls
     */
    public MohsHealthIndicator(MohsLifecycle lifecycle) {
        this.lifecycle = Objects.requireNonNull(lifecycle, "lifecycle");
    }

    @Override
    public Health health() {
        EngineState state = lifecycle.state();
        Status status = switch (state) {
            case RUNNING -> Status.UP;
            case STARTING, PAUSED, DRAINING -> Status.OUT_OF_SERVICE;
            case CREATED, STOPPED -> Status.DOWN;
        };
        return Health.status(status).withDetail("state", state.name()).build();
    }
}
