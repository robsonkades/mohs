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
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import io.mohs.core.ExecutionQuery;
import io.mohs.core.Mohs;
import io.mohs.rest.CursorPage;
import io.mohs.rest.execution.ExecutionSummaryResponse;
import io.mohs.rest.job.JobResponse;
import io.mohs.rest.node.NodeResponse;
import io.mohs.rest.runner.RunnerResponse;

/**
 * The snapshot stream behind {@code GET /overview/stream} — polling moved to the server, not event
 * delivery: each tick emits the complete current state as named SSE events ({@code overview},
 * {@code jobs}, {@code nodes}, {@code runners}, {@code executions}), each wrapped in a
 * {@link SnapshotEnvelope} ({@code {asOf, data}}), where {@code asOf} is the instant of the snapshot
 * — one for all five events of the tick.
 *
 * <p>A disconnection loses nothing: the next frame is the whole snapshot again. That is why this
 * endpoint does NOT violate the REST design's "no SSE in v1" decision: that decision rejected
 * best-effort event delivery without the future durable table, whereas a periodic snapshot promises
 * no durability at all.
 *
 * <p>One tick is shared by every subscriber (Observer, GoF): the READS cost the same as one
 * {@code GET /overview} plus the lists per interval, regardless of how many dashboards are
 * connected — and zero with no subscriber, since the tick returns before touching the database. The
 * reads block no writer and take no shared lock on any dialect, and the guarantee comes from row
 * versioning rather than hints: natively on PostgreSQL and MySQL, and on SQL Server because
 * {@code READ_COMMITTED_SNAPSHOT} is a boot requirement of that dialect — without it, one
 * uncommitted claim was measured blocking these counts to a lock timeout at this very cadence.
 *
 * <p>The SENDS are per subscriber, on a virtual thread (blocking network I/O — the house rule), with
 * <em>conflation</em>: if a client's previous frame has not finished writing (a slow client, a
 * suspended laptop with the connection still alive), the tick SKIPS that client. Queueing here would
 * work against the design itself, since the next frame is the complete snapshot again. Without it,
 * the slowest client's blocking servlet write would define everyone's latency (JCIP ch. 8: a task
 * with no time bound monopolising a fixed-cardinality executor) and a stuck send would hold the
 * {@code writeLock} that shutdown contends for.
 */
public final class OverviewStreamBroadcaster implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(OverviewStreamBroadcaster.class);

    /** The v1's fixed cadence, like {@link OverviewController#DEFAULT_THROUGHPUT_WINDOW} — it becomes a property when somebody needs another. Its ceiling is {@code MohsImpl.RECENT_WINDOW} (10s): above that, the overview's rate starts skipping work between samples. */
    static final Duration STREAM_INTERVAL = Duration.ofSeconds(2);

    /**
     * The ceiling on simultaneous subscribers. It is not capacity sizing: it is a guard against
     * amplification, because every new connection pays a full {@code buildFrames()} before joining
     * and the API has no built-in authentication. 64 is generous for real use (a handful of
     * operators with the dashboard open) and low enough that abuse hits the limit before the
     * database does.
     */
    static final int MAX_SUBSCRIBERS = 64;

    /** The ceiling on the read scope's quiescence ({@code closeScope}) — under failure, a snapshot lasts at most {@code STREAM_INTERVAL + FRAME_CANCEL_GRACE}. */
    static final Duration FRAME_CANCEL_GRACE = Duration.ofSeconds(1);

    /** Deep enough for the four-layer wrapping a disconnection arrives in, shallow enough to be a bound. */
    private static final int MAX_CAUSE_DEPTH = 16;

    private final Mohs mohs;
    private final Clock clock;
    private final CopyOnWriteArrayList<Subscriber> subscribers = new CopyOnWriteArrayList<>();
    private final ScheduledExecutorService scheduler;
    private final ExecutorService senders = Executors.newThreadPerTaskExecutor(
            Thread.ofVirtual().name("mohs-overview-sse-send-", 0).factory());

    /** The door closed from {@link #close} onwards — what depends on it is {@link #subscribe}, and its Javadoc says why. */
    private final AtomicBoolean closed = new AtomicBoolean(false);

    /** Package-visible for the tests: it builds WITHOUT a timer, so {@code tick()} can be called directly for full determinism (no sleeping, the house rule). */
    OverviewStreamBroadcaster(Mohs mohs, Clock clock) {
        this.mohs = Objects.requireNonNull(mohs, "mohs");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.scheduler = Executors.newSingleThreadScheduledExecutor(
                Thread.ofPlatform().name("mohs-overview-sse").daemon().factory());
    }

    /**
     * The factory that starts the timer (Effective Java, Item 1): scheduling outside the constructor
     * eliminates the this-escape (JCIP §3.2). It would be safe today, since a submit establishes
     * happens-before, but any future field assigned after the scheduling would be published to the
     * tick's thread without synchronisation.
     *
     * <p>Fixed-delay, not fixed-rate: a slow tick (a degraded database) must not accumulate a queue
     * of ticks that then fire in a burst.
     */
    public static OverviewStreamBroadcaster start(Mohs mohs, Clock clock) {
        OverviewStreamBroadcaster broadcaster = new OverviewStreamBroadcaster(mohs, clock);
        broadcaster.scheduler.scheduleWithFixedDelay(broadcaster::guardedTick,
                STREAM_INTERVAL.toMillis(), STREAM_INTERVAL.toMillis(), TimeUnit.MILLISECONDS);
        return broadcaster;
    }

    /**
     * Registers the client and sends the initial snapshot immediately, so the dashboard paints
     * without waiting for the first tick. The initial send is synchronous on purpose: before Spring
     * initialises the emitter, {@code send} only buffers in memory — there is no network write to
     * block on. Without a timeout ({@code 0L}) the stream lives until the client disconnects, and
     * the callbacks remove the subscriber on both possible outcomes.
     *
     * <p>After {@link #close} it returns an ALREADY completed emitter without touching the database:
     * the client sees the end of the stream and the async request closes immediately. That is not
     * defensiveness — it is the mandatory outcome of a race that shutdown makes likely. The case
     * requiring it is not the client that ARRIVES afterwards (the connector no longer accepts a new
     * connection one phase later), it is the one ALREADY HERE: a {@code subscribe} stuck in
     * {@link #buildFrames}'s reads — tens or hundreds of milliseconds, against a database that is
     * also going away — while {@code close} runs. A subscriber registered after that would stay mute
     * forever (the timer having stopped) and, worse, would go back to holding up the graceful
     * shutdown as an active async request — exactly what the phase ordering exists to close.
     *
     * <p>There are two reads of {@code closed}, each with its own role: the first avoids
     * {@link #buildFrames} (four database reads) at a closed door; the second is what closes the
     * race — publishing the subscriber BEFORE re-reading the flag guarantees that either
     * {@code close} finds it in the list, or we find the flag, never neither.
     */
    public SseEmitter subscribe() {
        SseEmitter emitter = new SseEmitter(0L);
        if (closed.get() || subscribers.size() >= MAX_SUBSCRIBERS) {
            // An explicit ceiling: every CONNECTION pays a whole buildFrames() (four database reads,
            // including the backlog scan) before registering, and the API has no authentication —
            // without a ceiling, a curl loop amplifies against the same database the engine's claim
            // uses. The TICK's cost is shared, and measured; the cost of JOINING is per connection,
            // and was never measured.
            return endedStream(emitter);
        }
        // Born BUSY: the initial snapshot goes out outside the conflation CAS, so a concurrent tick
        // could interleave its frames with these and deliver the OLD frame last. The API contract
        // admits that reorder and at the same time tells the client not to defend against it by
        // comparing asOf — the one able to not produce it is the server.
        Subscriber subscriber = new Subscriber(emitter, new AtomicBoolean(true));
        emitter.onCompletion(() -> subscribers.remove(subscriber));
        emitter.onError(_ -> subscribers.remove(subscriber));
        // The snapshot BEFORE registering: if the read fails (a degraded database), the exception
        // becomes the subscribe's 500 without leaving an orphan subscriber in the list — an emitter
        // that was never initialised fires no callback and would buffer every future tick's sends
        // with no ceiling.
        List<Frame> initialSnapshot = buildFrames();
        subscribers.add(subscriber);
        if (closed.get()) {
            subscribers.remove(subscriber);
            return endedStream(emitter);
        }
        try {
            send(subscriber, initialSnapshot);
        } finally {
            subscriber.inFlight().set(false);
        }
        return emitter;
    }

    /** What {@link #subscribe} returns at a closed door: a stream born already finished — the client reads end-of-stream and the async request closes immediately. */
    private static SseEmitter endedStream(SseEmitter emitter) {
        completeQuietly(emitter);
        return emitter;
    }

    /**
     * Package-private for deterministic testing — the scheduling only wraps this.
     *
     * <p>Synchronous by construction: fixed-delay plus the read scope's quiescence guarantee that one
     * tick never overlaps another. Against a degraded database the churn stays at four connections
     * per roughly {@code STREAM_INTERVAL + FRAME_CANCEL_GRACE}, orders of magnitude below what the
     * engine's own poll does under the same degradation; the right layer for a finer timeout is
     * {@code Statement.setQueryTimeout} in the store.
     */
    void tick() {
        if (subscribers.isEmpty()) {
            return;
        }
        List<Frame> frames = buildFrames();
        for (Subscriber subscriber : subscribers) {
            // Conflation: a previous frame still in flight means a slow client, so skip it — it
            // delays only itself, and the next frame is the whole snapshot again
            if (subscriber.inFlight().compareAndSet(false, true)) {
                sendOnItsOwnThread(subscriber, frames);
            }
        }
    }

    /**
     * Hands one client's frames to its virtual thread, with the caller's {@code inFlight} CAS already
     * won — the flag is released on every outcome, including the one where the send never starts.
     */
    private void sendOnItsOwnThread(Subscriber subscriber, List<Frame> frames) {
        try {
            senders.submit(() -> {
                try {
                    send(subscriber, frames);
                } finally {
                    subscriber.inFlight().set(false);
                }
            });
        } catch (RejectedExecutionException shuttingDown) {
            // A close() in progress: hand the flag back — without this, if the lifecycle ever
            // allowed a tick after close, the subscriber would stay mute forever (silent
            // starvation)
            subscriber.inFlight().set(false);
        }
    }

    /**
     * A read failure must not kill the scheduling ({@code ScheduledExecutorService} cancels a task
     * that throws) — a {@link RuntimeException} becomes a log line and the next tick tries again.
     * {@link Error} is the opposite: log what broke and RETHROW (see below).
     */
    private void guardedTick() {
        try {
            tick();
        } catch (RuntimeException e) {
            log.warn("overview stream tick failed — clients keep their connection, retrying on the next tick ({})",
                    STREAM_INTERVAL, e);
        } catch (Error e) {
            // scheduleWithFixedDelay CANCELS a throwing task, permanently. With timeout-0L emitters,
            // every connected dashboard would freeze on stale data with no end of stream and not one
            // line of log. buildFrames rethrows an Error coming out of ExecutionException, so there
            // is a real path down to here
            log.error("overview stream tick died with an Error — the periodic task is CANCELLED and every connected"
                    + " dashboard will silently freeze on stale data. Restart the application.", e);
            throw e;
        }
    }

    /**
     * The current snapshot, one named event per type — a client subscribes only to what it consumes
     * ({@code EventSource.addEventListener}). The {@code executions} frame uses
     * {@link CursorPage#DEFAULT_PAGE_SIZE} on purpose: the dashboard's panel shows the SAME slice as
     * the first page of {@code GET /executions}, and if they ever diverge, the divergence should be a
     * decision rather than an accident.
     *
     * <p>The five reads are independent — a structural fan-out in the shape of JEP 505 (fork the
     * subtasks, join with a deadline, one failure cancels its siblings), but with the stable API:
     * {@code StructuredTaskScope} is preview on JDK 25 and {@code --enable-preview} would pin the
     * library's host to one exact JDK (a project rule — the migration is mechanical once it
     * finalises). Snapshot latency is the slowest read, not the sum; and a deadline of one
     * {@link #STREAM_INTERVAL} (a snapshot that does not close within a tick is already stale)
     * guarantees a hung read never freezes the timer forever.
     *
     * <p>The scope is an executor PER CALL, closed in the {@code finally} with bounded quiescence
     * ({@link #FRAME_CANCEL_GRACE}) — the central guarantee of a structural join: NO subtask outlives
     * the scope. An earlier version waited for nothing: {@code cancel(true)} returns immediately with
     * a sibling thread still reading the database — cancellation is a two-phase protocol, signal AND
     * await (JCIP §7.1.5). Under failure, the snapshot's worst case becomes
     * {@code STREAM_INTERVAL + FRAME_CANCEL_GRACE}.
     */
    private List<Frame> buildFrames() {
        long deadlineNanos = System.nanoTime() + STREAM_INTERVAL.toNanos();
        // One asOf per snapshot, from the injected clock (the house invariant): the five events of
        // the same tick carry the SAME instant — it is the stamp that lets the frontend order or
        // discard a late frame, and diverging between events of one snapshot would only create false
        // precedence.
        Instant asOf = clock.instant();
        ExecutorService scope = Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("mohs-overview-frame-", 0).factory());
        try {
            List<Future<Frame>> forks = List.of(
                    fork(scope, "overview", asOf, () -> OverviewResponse.from(mohs.overview(OverviewController.DEFAULT_THROUGHPUT_WINDOW))),
                    fork(scope, "jobs", asOf, () -> mohs.jobs().stream().map(JobResponse::from).toList()),
                    fork(scope, "nodes", asOf, () -> mohs.nodes().stream().map(NodeResponse::from).toList()),
                    // runners alongside the rest: it is what carries the per-runner CEILING (max),
                    // and concurrency without a denominator cannot say whether 59 is slack or
                    // saturation. An in-memory read of the RunnerRegistry, with no query — the
                    // cheapest of the five frames.
                    fork(scope, "runners", asOf, () -> mohs.runners().stream().map(RunnerResponse::from).toList()),
                    fork(scope, "executions", asOf, () -> mohs
                            .executions(new ExecutionQuery(null, null, null, null, null, CursorPage.DEFAULT_PAGE_SIZE))
                            .stream().map(ExecutionSummaryResponse::from).toList()));

            List<Frame> frames = new ArrayList<>(forks.size());

            for (Future<Frame> fork : forks) {
                frames.add(fork.get(deadlineNanos - System.nanoTime(), TimeUnit.NANOSECONDS));
            }

            return List.copyOf(frames);
        } catch (ExecutionException e) {
            // launderThrowable (JCIP §5.5.2): the original exception is preserved where possible —
            // RestExceptionHandler maps by type and the Future's wrapper is nobody's contract; an
            // Error never becomes an "illegal state".
            switch (e.getCause()) {
                case RuntimeException runtime -> throw runtime;
                case Error error -> throw error;
                // Theoretically null (FutureTask always wraps a non-null cause) — but a pattern switch
                // throws NPE on a null selector, and the guard costs nothing
                case null, default -> throw new IllegalStateException("snapshot frame read failed", e.getCause());
            }
        } catch (TimeoutException e) {
            throw new IllegalStateException("snapshot frame reads exceeded " + STREAM_INTERVAL + " — database degraded?", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while building the snapshot frame", e);
        } finally {
            closeScope(scope);
        }
    }

    /** One subtask of the fan-out: the read runs on a virtual thread of THIS call's scope, and the result is born as a named event, enveloped with the snapshot's {@code asOf}. */
    private static Future<Frame> fork(ExecutorService scope, String eventName, Instant asOf, Supplier<Object> read) {
        return scope.submit(() -> new Frame(eventName, new SnapshotEnvelope(asOf, read.get())));
    }

    /**
     * The end of the scope, on any outcome: interrupt whatever is left ({@code shutdownNow}) and
     * AWAIT quiescence with a ceiling — on the happy path nobody is left and the return is
     * immediate.
     *
     * <p>A reader ignoring the interrupt beyond the grace (embedded H2, with no socket, may not
     * honour an interrupt) becomes a leaked-thread WARN, never an unbounded wait — the timer stays
     * protected.
     */
    private static void closeScope(ExecutorService scope) {
        scope.shutdownNow();
        try {
            if (!scope.awaitTermination(FRAME_CANCEL_GRACE.toMillis(), TimeUnit.MILLISECONDS)) {
                log.warn("snapshot frame reader ignored cancellation for {} — leaked reader thread", FRAME_CANCEL_GRACE);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Writes the frames to one subscriber; a failure removes it immediately without taking the others
     * down.
     *
     * <p>The severity is decided by {@link #endOfStream}, never by the exception's outermost type.
     * That distinction is the whole point: this Javadoc used to say an {@code IOException} was the
     * client leaving and a {@code RuntimeException} was our defect, and that sentence is what produced
     * the bug — a disconnection reaches here wrapped by the message converter, so the type on top says
     * nothing about whose fault it is.
     *
     * <p>A defect of OURS keeps the WARN with the cause: it would take down client after client, and
     * without the cause it becomes an undiagnosable failure at 3 a.m.
     */
    private void send(Subscriber subscriber, List<Frame> frames) {
        try {
            for (Frame frame : frames) {
                subscriber
                        .emitter()
                        .send(SseEmitter.event().name(frame.name()).data(frame.data(), MediaType.APPLICATION_JSON));
            }
        } catch (IOException | RuntimeException failure) {
            if (endOfStream(failure)) {
                log.debug("SSE client dropped during send", failure);
            } else {
                log.warn("dropping SSE subscriber after server-side send failure", failure);
            }
            subscribers.remove(subscriber);
            completeQuietly(subscriber.emitter());
        }
    }

    /**
     * Whether a failed send means the STREAM is over rather than a defect of ours, decided by the cause
     * chain and by shape — because the outermost type does not say.
     *
     * <p>{@code SseEmitter.send} propagates a bare {@code IOException} only while the write fails
     * before the message converter. Every frame here goes through Jackson, so the same disconnection
     * arrives wrapped: {@code IllegalStateException("Failed to send …")} →
     * {@code HttpMessageNotWritableException} → {@code JacksonIOException} →
     * {@code AsyncRequestNotUsableException}. Catching {@code IOException} therefore never fired on
     * the path that actually happens, and the most routine event in SSE — a tab closed, a laptop
     * suspended, a proxy timing out — was logged as a server-side defect, with a stack trace, on
     * every disconnection.
     *
     * <p>Both markers extend {@code IOException} ({@code AsyncRequestNotUsableException} in Spring,
     * {@code ClientAbortException} in Tomcat), so scanning for that one type covers every container
     * without naming any of them.
     */
    static boolean endOfStream(Throwable failure) {
        // Assert.state in ResponseBodyEmitter.send, which carries no cause at all: the emitter was
        // already completed — a concurrent close(), a disconnection the container saw first. The
        // stream is over, and a serialisation defect of ours never takes this shape, because
        // sendInternal always wraps one as "Failed to send …" WITH the cause attached
        if (failure instanceof IllegalStateException && failure.getCause() == null) {
            return true;
        }
        // Bounded rather than while(cause != null): a cause CYCLE is constructible (initCause refuses
        // self-causation, but not a → b → a) and would spin the sender's virtual thread forever
        // instead of dropping one subscriber — a hang, not a log line. Overrunning the bound answers
        // false, so the failure mode is a noisy WARN rather than silence
        Throwable cause = failure;
        for (int depth = 0; cause != null && depth < MAX_CAUSE_DEPTH; depth++) {
            if (cause instanceof IOException) {
                return true;
            }
            cause = cause.getCause();
        }
        return false;
    }

    /**
     * Ends one stream, ALWAYS with {@code complete()} and never with {@code completeWithError} — the
     * distinction the failure deserves is already in this class's own log line, one frame above.
     *
     * <p>{@code completeWithError} dispatches the request back through the container so the exception
     * reaches {@code @ExceptionHandler}. On an SSE response that can only produce noise: the headers
     * went out with the first frame, so the response is committed as {@code text/event-stream} and the
     * error pipeline cannot render anything into it. What it produced instead, for every closed
     * browser tab, was a pair — {@code RestExceptionHandler} logging "unhandled exception reaching the
     * REST layer" at ERROR with the send's whole stack, then
     * {@code HttpMessageNotWritableException: No converter for [ProblemDetail] with preset Content-Type
     * 'text/event-stream'} at WARN as it failed to write the reply. Two stack traces per disconnection,
     * for something nobody can receive.
     *
     * <p>Removal is unaffected: {@code subscribe} registers {@code onCompletion} AND {@code onError},
     * so the subscriber leaves the list either way.
     */
    static void completeQuietly(SseEmitter emitter) {
        try {
            emitter.complete();
        } catch (RuntimeException alreadyClosed) {
            // The emitter may have reached its own outcome concurrently (a disconnection at the same
            // instant) — the second outcome must not cost the others theirs
            log.debug("emitter already closed while being discarded", alreadyClosed);
        }
    }

    /**
     * A clean shutdown: close the door to new subscribers, stop the timer and complete the open
     * streams — the client sees an end of stream, not a dead connection. The completes go through the
     * sending virtual threads: a stuck client holding its own emitter's {@code writeLock} delays only
     * itself, never the shutdown.
     *
     * <p>What calls this at the right moment is the auto-configuration's {@code SmartLifecycle}, one
     * phase ABOVE the web server's graceful shutdown: with timeout {@code 0L} the stream's async
     * requests do not end by themselves, and a container waiting on active requests would spend the
     * whole {@code spring.lifecycle.timeout-per-shutdown-phase} before giving up. The bean's destroy
     * method calls it again at the end of the context — which is why the flag also makes closing
     * idempotent, as {@link AutoCloseable#close()}'s contract recommends.
     */
    @Override
    public void close() {
        // The flag BEFORE the list: it is what gives a concurrent subscribe the guarantee that one of
        // the two sides sees the other
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        scheduler.shutdownNow();
        for (Subscriber subscriber : subscribers) {
            senders.submit(() -> completeQuietly(subscriber.emitter()));
        }
        subscribers.clear();
        senders.shutdown();
    }

    private record Frame(String name, SnapshotEnvelope data) {
    }

    /**
     * The wire envelope of every stream event: {@code asOf} is the snapshot's instant (one for all
     * five events of the same tick) — the frontend uses it to order frames, discard a late one and
     * distribute updates internally without depending on the client's clock.
     */
    record SnapshotEnvelope(Instant asOf, Object data) {
    }

    /** An emitter plus the conflation flag — {@code inFlight} is the CAS guaranteeing at most one send in flight per client. */
    private record Subscriber(SseEmitter emitter, AtomicBoolean inFlight) {
    }
}
