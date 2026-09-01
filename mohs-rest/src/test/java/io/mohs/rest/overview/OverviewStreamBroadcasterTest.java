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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.http.converter.HttpMessageNotWritableException;
import tools.jackson.core.exc.JacksonIOException;
import org.springframework.web.context.request.async.AsyncRequestNotUsableException;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import io.mohs.core.Mohs;
import io.mohs.core.OverviewSnapshot;
import io.mohs.core.ThroughputReading;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
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
     * The severity split, pinned on the shape the container ACTUALLY produces. This is the failure
     * that shipped: {@code send} caught {@code IOException} for "the client went away", but every
     * frame goes through Jackson, so a disconnection arrives as
     * {@code IllegalStateException → HttpMessageNotWritableException → AsyncRequestNotUsableException}
     * — a {@code RuntimeException}. The bare-{@code IOException} branch never fired on the real path,
     * and a closed browser tab was logged as a server-side defect with a stack trace, every time.
     *
     * <p>Reproduced verbatim from a production trace rather than approximated: it is the wrapping
     * that was misread, so a test that threw a bare {@code IOException} would have passed against the
     * broken code — which is exactly why the bug survived a suite this file already had.
     */
    @Test
    void aDisconnectionWrappedByTheMessageConverterIsStillTheClientGoing() {
        Throwable asSpringWrapsIt = new IllegalStateException("Failed to send [DataWithMediaType@425cbff]",
                new HttpMessageNotWritableException("Could not write JSON",
                        JacksonIOException.construct(
                                new AsyncRequestNotUsableException("ServletOutputStream failed to write"))));

        assertThat(OverviewStreamBroadcaster.endOfStream(asSpringWrapsIt)).isTrue();
    }

    /**
     * The other shape {@code ResponseBodyEmitter.send} can fail with, and the one the first fix
     * missed: {@code Assert.state(!this.complete, …)} throws an {@code IllegalStateException} with NO
     * cause at all. It is routine, not exotic — {@code close()} submits the completion on the same
     * executor a tick's send may still be running on, so the two race for the emitter's
     * {@code writeLock}, and a frame that arrives after the completion wins nothing.
     *
     * <p>Classified as our defect it would print a stack trace per connected dashboard on every
     * graceful shutdown. A serialisation defect of ours never takes this shape: {@code sendInternal}
     * always wraps one as {@code "Failed to send …"} WITH the cause attached.
     */
    @Test
    void aSendRefusedByAnAlreadyCompletedEmitterIsTheStreamEnding() {
        Throwable asAssertStateThrowsIt = new IllegalStateException("ResponseBodyEmitter has already completed");

        assertThat(OverviewStreamBroadcaster.endOfStream(asAssertStateThrowsIt)).isTrue();
    }

    /** The unwrapped form still counts — a write that fails before the converter never reaches Jackson. */
    @Test
    void aBareIoExceptionIsTheClientGoing() {
        assertThat(OverviewStreamBroadcaster.endOfStream(new IOException("broken pipe"))).isTrue();
    }

    /**
     * A discarded stream ends with {@code complete()}, NEVER {@code completeWithError} — the guard on
     * the noisiest defect this class had.
     *
     * <p>{@code completeWithError} dispatches the request back through the container so the exception
     * reaches {@code @ExceptionHandler}. On SSE the headers went out with the first frame, so the
     * response is committed as {@code text/event-stream} and no {@code ProblemDetail} can be written
     * into it. Every closed browser tab therefore produced two stack traces nobody could receive:
     * {@code RestExceptionHandler} at ERROR ("unhandled exception reaching the REST layer"), then
     * {@code No converter for [ProblemDetail] with preset Content-Type 'text/event-stream'} at WARN.
     *
     * <p>Asserted on the decision itself rather than through a subscribe/send round trip: the
     * subscriber's emitter is created inside {@code subscribe} and cannot be substituted, so a test
     * that stood a mock beside it and checked the mock stayed untouched would pass whatever the code
     * did.
     */
    @Test
    void aDiscardedStreamEndsWithoutFeedingTheMvcErrorPipeline() {
        SseEmitter emitter = mock(SseEmitter.class);

        OverviewStreamBroadcaster.completeQuietly(emitter);

        verify(emitter).complete();
        verify(emitter, never()).completeWithError(any());
    }

    /**
     * The other half, and the reason this is a split rather than a blanket demotion: a serialisation
     * defect of ours would take down client after client, so it keeps the WARN and the cause. Nothing
     * in its chain is an {@code IOException}.
     */
    @Test
    void aServerSideFailureIsNotTheClientGoing() {
        Throwable ourBug = new IllegalStateException("frame serialisation failed",
                new IllegalArgumentException("no serializer for FrameDto"));

        assertThat(OverviewStreamBroadcaster.endOfStream(ourBug)).isFalse();
    }

    /**
     * A cause CYCLE is constructible — {@code initCause} refuses self-causation but not
     * {@code a → b → a}. Without the depth bound this walk would spin on the sender's virtual thread
     * instead of dropping one subscriber: a hang, not a log line. The test would not fail, it would
     * never return, which is why the bound is a bound and not a self-comparison.
     */
    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void aCyclicCauseChainTerminates() {
        Exception first = new IllegalStateException("first");
        Exception second = new IllegalStateException("second", first);
        first.initCause(second);

        assertThat(OverviewStreamBroadcaster.endOfStream(first)).isFalse();
    }

    /**
     * The policy belongs to the CLASS, not to one helper: {@code completeQuietly} is guarded by the
     * test above, but a future call site writing {@code emitter.completeWithError(failure)} inline —
     * which is, in essence, what the broken code did — would walk straight past it and bring the four
     * log events back.
     *
     * <p>A source scan rather than a behavioural test, and for the same reason
     * {@code TerminalStateWriteScanTest} is one in {@code mohs-store-jdbc}: what has to be forbidden
     * is a CALL that must not appear, and no assertion over behaviour can see a call that nobody
     * made yet.
     */
    @Test
    void noStreamIsEverEndedThroughTheMvcErrorPipeline() throws IOException {
        Path source = Path.of("src/main/java/io/mohs/rest/overview/OverviewStreamBroadcaster.java");

        assertThat(Files.readString(source))
                .as("completeWithError dispatches the request back through the MVC error pipeline, "
                        + "which on a response already committed as text/event-stream renders nothing "
                        + "and logs an ERROR plus a WARN per disconnection — end the stream with "
                        + "complete() and let this class's own log line carry the severity")
                .doesNotContain("completeWithError(");
    }

    /**
     * An emitter completed before Spring initialises it fires no {@code onCompletion} — there is no
     * handler yet. What the public API exposes of that state is {@code send}, which refuses after the
     * end of the stream.
     */
    private static void assertEndedStream(SseEmitter emitter) {
        assertThatThrownBy(() -> emitter.send("probe"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("already completed")
                // The public tell of WHICH completion ran: completeWithError records the failure, and
                // Assert.state then appends "with error: …" to its refusal; complete() records
                // nothing. One line promotes the three close()-related tests above into guards on the
                // never-completeWithError policy, through the public API and with no mock
                .hasMessageNotContaining("with error");
    }
}
