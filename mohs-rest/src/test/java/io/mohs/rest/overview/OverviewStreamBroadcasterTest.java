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
package io.mohs.rest.overview;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import io.mohs.core.Mohs;
import io.mohs.core.OverviewSnapshot;
import io.mohs.core.ThroughputReading;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/** The stream's cost contract: zero reads with no subscriber; one set of reads per tick, shared — never per client. */
class OverviewStreamBroadcasterTest {

    /** The overview's short reading — fixed; these tests verify the long one. */
    private static final ThroughputReading RECENT = new ThroughputReading(Duration.ofSeconds(10), 0L, 0L);

    private static final Instant NOW = Instant.parse("2026-08-17T12:00:00Z");

    private final Mohs mohs = mock(Mohs.class);
    private final OverviewStreamBroadcaster broadcaster =
            new OverviewStreamBroadcaster(mohs, Clock.fixed(NOW, ZoneOffset.UTC));

    @AfterEach
    void closeBroadcaster() {
        broadcaster.close();
    }

    private void stubSnapshotReads() {
        when(mohs.overview(any())).thenReturn(
                new OverviewSnapshot(Map.of(), new ThroughputReading(Duration.ofSeconds(60), 0L, 0L), RECENT));
        when(mohs.jobs()).thenReturn(List.of());
        when(mohs.nodes()).thenReturn(List.of());
        when(mohs.executions(any())).thenReturn(List.of());
    }

    @Test
    void tickWithoutSubscribersNeverTouchesTheFacade() {
        broadcaster.tick();

        verifyNoInteractions(mohs);
    }

    @Test
    void subscribeSendsTheInitialSnapshotImmediately() {
        stubSnapshotReads();

        assertThat(broadcaster.subscribe()).isNotNull();

        verify(mohs).overview(OverviewController.DEFAULT_THROUGHPUT_WINDOW);
        verify(mohs).jobs();
        verify(mohs).nodes();
        verify(mohs).executions(any());
    }

    /**
     * An initial snapshot that fails (a degraded database) becomes the subscribe's 500 WITHOUT
     * leaving an orphan emitter registered — an emitter that was never initialised fires no removal
     * callback and would buffer every future tick with no ceiling.
     */
    @Test
    void aFailedInitialSnapshotDoesNotLeakTheEmitter() {
        when(mohs.overview(any())).thenThrow(new IllegalStateException("database unavailable"));

        assertThatThrownBy(broadcaster::subscribe).isInstanceOf(IllegalStateException.class);

        clearInvocations(mohs);
        broadcaster.tick();
        verifyNoInteractions(mohs);
    }

    /**
     * {@code close()}'s closed door: whoever arrives afterwards receives a stream born already
     * finished, rather than a mute subscriber (the timer having stopped) that would go back to
     * holding up the graceful shutdown as an active async request. This is the easy half of the
     * guard — the one that matters is in the next test.
     */
    @Test
    void subscribeAfterCloseReturnsAnAlreadyEndedStream() {
        // With the reads stubbed, a subscribe without the closed door would walk the happy path and
        // return a LIVE stream — which is what makes this test genuinely fail when the guard is
        // removed
        stubSnapshotReads();
        broadcaster.close();

        assertEndedStream(broadcaster.subscribe());
    }

    /**
     * The race the re-read of {@code closed} exists to close: {@code close} enters AFTER the first
     * guard and BEFORE the subscriber is registered — a window the initial snapshot's four reads
     * throw wide open. Without the re-read, this subscribe would return a live stream nobody would
     * ever complete, and the graceful shutdown would go back to waiting on it until the phase's
     * timeout.
     */
    @Test
    void subscribeRacingWithCloseEndsTheStreamInstead() {
        stubSnapshotReads();
        when(mohs.jobs()).thenAnswer(_ -> {
            broadcaster.close();
            return List.of();
        });

        assertEndedStream(broadcaster.subscribe());
    }

    /** The bean's destroy method calls {@code close()} again after the {@code SmartLifecycle}: the second close neither throws nor reopens the door. */
    @Test
    void closingAgainKeepsTheDoorShut() {
        stubSnapshotReads();
        broadcaster.close();

        broadcaster.close();

        assertEndedStream(broadcaster.subscribe());
    }

    /**
     * An emitter completed before Spring initialises it fires no {@code onCompletion} — there is no
     * handler yet. What the public API exposes of that state is {@code send}, which refuses after the
     * end of the stream.
     */
    private static void assertEndedStream(SseEmitter emitter) {
        assertThatThrownBy(() -> emitter.send("probe"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("already completed");
    }
}
