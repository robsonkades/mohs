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

import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;

import io.mohs.core.EngineState;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

class EngineStartupDelayTest {

    private final WorkQueue queue = mock(WorkQueue.class);
    private final Dispatcher dispatcher = mock(Dispatcher.class);
    private final HistoryStore history = mock(HistoryStore.class);
    private final LeaseStore leases = mock(LeaseStore.class);
    private final JobStore jobs = mock(JobStore.class);
    private final NodeStore nodes = mock(NodeStore.class);
    private final TriggerFirer firer = mock(TriggerFirer.class);
    private final RateLimitStore limits = mock(RateLimitStore.class);

    private EngineSettings settings(Duration delay) {
        return new EngineSettings(Duration.ofMillis(25), Duration.ofSeconds(2), 50, 64, 1,
                Duration.ofSeconds(30), Duration.ofSeconds(15), null, Duration.ofSeconds(60),
                Duration.ZERO, Duration.ZERO, delay);
    }

    private Engine engine(Duration delay) {
        return new Engine(queue, dispatcher, history, leases, jobs, nodes, firer,
                mock(ExecutionWindowRegistry.class), limits, Clock.systemUTC(), settings(delay),
                mock(RunnerRegistry.class), mock(EngineMetrics.class));
    }

    @Test
    void noTickBeforeDelayEvenWhenWorkSignalsWakeTheLoop() throws Exception {
        Duration delay = Duration.ofMillis(300);
        Engine engine = engine(delay);
        CountDownLatch firstHeartbeat = new CountDownLatch(1);
        AtomicLong firstTick = new AtomicLong();
        doAnswer(invocation -> {
            firstTick.compareAndSet(0, System.nanoTime());
            firstHeartbeat.countDown();
            return null;
        }).when(nodes).heartbeat(anyString(), any(), anyLong(), any(), any());
        long began = System.nanoTime();
        engine.start();
        try {
            assertThat(engine.state()).isEqualTo(EngineState.STARTING);
            for (int i = 0; i < 5; i++) {
                engine.signalWorkScheduled();
                assertThat(firstHeartbeat.await(20, TimeUnit.MILLISECONDS)).isFalse();
            }
            verifyNoInteractions(queue, dispatcher, history, leases, jobs, nodes, firer, limits);
            assertThat(firstHeartbeat.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(firstTick.get() - began).isGreaterThanOrEqualTo(delay.toNanos());
            assertThat(engine.state()).isEqualTo(EngineState.RUNNING);
        } finally {
            engine.stop(Duration.ofSeconds(1));
        }
    }

    @Test
    void stopCancelsPendingStartupWithoutAnyStoreOperation() {
        Engine engine = engine(Duration.ofHours(1));
        engine.start();
        assertThatThrownBy(engine::start).isInstanceOf(IllegalStateException.class);
        engine.stop(Duration.ZERO);
        engine.signalWorkScheduled();
        assertThat(engine.state()).isEqualTo(EngineState.STOPPED);
        verifyNoInteractions(queue, dispatcher, history, leases, jobs, nodes, firer, limits);
        assertThatThrownBy(engine::start).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void zeroPreservesImmediateStartup() {
        Engine engine = engine(Duration.ZERO);
        engine.start();
        try {
            assertThat(engine.state()).isEqualTo(EngineState.RUNNING);
        } finally {
            engine.stop(Duration.ofSeconds(1));
        }
    }

    @Test
    void rejectsNegativeAndOverflowingDurations() {
        assertThatThrownBy(() -> settings(Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("startup-delay");
        assertThatThrownBy(() -> settings(Duration.ofSeconds(Long.MAX_VALUE)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("startup-delay");
    }
}
