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

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import org.junit.jupiter.api.Test;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import org.springframework.context.SmartLifecycle;

import io.mohs.core.Mohs;
import io.mohs.rest.overview.OverviewStreamBroadcaster;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

/**
 * What this adapter delivers is ORDER: the SSE streams must be completed before the web server
 * starts waiting on active requests, otherwise shutdown spends the whole
 * {@code spring.lifecycle.timeout-per-shutdown-phase} waiting on a stream that — with timeout
 * {@code 0L} — never ends by itself.
 *
 * <p>The closing behaviour itself (a closed door, idempotence, the race with {@code subscribe})
 * belongs to the broadcaster and lives with it, in {@code OverviewStreamBroadcasterTest}.
 */
class MohsOverviewStreamLifecycleTest {

    private static final Instant NOW = Instant.parse("2026-08-29T02:33:15Z");

    private final OverviewStreamBroadcaster broadcaster =
            OverviewStreamBroadcaster.start(mock(Mohs.class), Clock.fixed(NOW, ZoneOffset.UTC));
    private final MohsOverviewStreamLifecycle lifecycle = new MohsOverviewStreamLifecycle(broadcaster);

    /**
     * Ties {@link MohsOverviewStreamLifecycle#PHASE}'s literal to Boot's real constant — this test
     * is what allows production not to depend on {@code spring-boot-web-server} (see the field's
     * Javadoc).
     *
     * <p>A HIGHER phase stops FIRST, so {@code +1} is the latest position that still precedes
     * graceful shutdown; and it is below the engine ({@link SmartLifecycle#DEFAULT_PHASE}), which
     * the dashboard should keep watching drain.
     */
    @Test
    void stopsBetweenTheEngineDrainAndTheWebServerGracefulShutdown() {
        assertThat(lifecycle.getPhase())
                .isEqualTo(WebServerApplicationContext.GRACEFUL_SHUTDOWN_PHASE + 1)
                .isLessThan(SmartLifecycle.DEFAULT_PHASE);
    }

    /** The adapter does nothing beyond closing the broadcaster at the right moment — and no longer declaring itself running. */
    @Test
    void stopClosesTheBroadcaster() {
        lifecycle.stop();

        // Genuinely closed: whoever arrives afterwards receives an already-finished stream
        assertThatThrownBy(() -> broadcaster.subscribe().send("probe"))
                .isInstanceOf(IllegalStateException.class);
        assertThat(lifecycle.isRunning()).isFalse();
    }
}
