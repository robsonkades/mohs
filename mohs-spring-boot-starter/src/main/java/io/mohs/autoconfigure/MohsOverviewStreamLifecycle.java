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

import org.springframework.context.SmartLifecycle;

import io.mohs.rest.overview.OverviewStreamBroadcaster;

/**
 * Fits closing the SSE stream ({@code GET /overview/stream}) into Spring's shutdown order, one
 * phase ABOVE the web server's graceful shutdown.
 *
 * <p>Without it, the broadcaster's only shutdown is the bean's destroy method, which runs in
 * {@code destroyBeans()} — after ALL of {@code stopBeans()} — and the arithmetic does not work out:
 * emitters are created with timeout {@code 0L} (they live until the client disconnects), the
 * container's graceful shutdown waits while any async request is active (on Tomcat,
 * {@code StandardContext#getInProgressAsyncCount() > 0}), and one open dashboard held the entire
 * phase until {@code spring.lifecycle.timeout-per-shutdown-phase} (30s by Boot's default) expired —
 * at which point the container aborted the graceful shutdown and dropped the connections by force.
 * With this lifecycle, the container reaches its phase with no async request left and the phase
 * costs milliseconds.
 *
 * <p>One residual case stays uncovered, deliberately: a client stuck in a {@code send} of the last
 * tick holds that emitter's own {@code writeLock}, and the {@code complete} queues behind it — that
 * stream again costs the container the whole phase, which aborts the graceful shutdown and drops
 * the connection. That is the right outcome: closing it would require {@code stop} to wait for the
 * completes, coupling shutdown latency to the slowest client — exactly what the broadcaster's
 * conflation avoids on the normal path.
 *
 * <p>A shutdown-only adapter, as {@link MohsEngineLifecycle} is for the engine, and for the same
 * reason: {@link SmartLifecycle}'s shape is not the collaborator's shape.
 */
final class MohsOverviewStreamLifecycle implements SmartLifecycle {

    /**
     * A copy of {@code WebServerApplicationContext.GRACEFUL_SHUTDOWN_PHASE}, written as a literal
     * on purpose, and the reason is the COMPILE CLASSPATH: that constant lives in
     * {@code spring-boot-web-server}, which does not reach here —
     * {@code spring-boot-starter-webmvc} brings {@code spring-boot-webmvc}/{@code spring-webmvc}
     * and stops there; {@code spring-boot-web-server} only enters in test scope, through
     * {@code spring-boot-webmvc-test}.
     *
     * <p>Importing it would cost one more dependency to read an {@code int} that would not even
     * exist at runtime: a compile-time constant is inlined into the class file (JLS §13.1), so no
     * reference to {@code WebServerApplicationContext} would survive javac anyway. What ties this
     * literal to Boot's real constant is {@code MohsOverviewStreamLifecycleTest}, where the
     * dependency does exist.
     */
    private static final int WEB_SERVER_GRACEFUL_SHUTDOWN_PHASE = SmartLifecycle.DEFAULT_PHASE - 1024;

    /** A higher phase stops first: one above graceful shutdown is the latest position that still precedes the container's wait for active requests. */
    static final int PHASE = WEB_SERVER_GRACEFUL_SHUTDOWN_PHASE + 1;

    private final OverviewStreamBroadcaster broadcaster;
    private volatile boolean running = true;

    MohsOverviewStreamLifecycle(OverviewStreamBroadcaster broadcaster) {
        this.broadcaster = Objects.requireNonNull(broadcaster, "broadcaster");
    }

    /**
     * A deliberate no-op: the broadcaster comes up already running from the {@code @Bean} factory,
     * which is why {@link #isRunning()} starts {@code true} and the processor never calls this on
     * refresh. Reviving is not an option either — {@link OverviewStreamBroadcaster#close()} shuts
     * the timer down for good. A context restart leaves the stream stopped, and {@code isRunning}
     * then reports that instead of lying.
     */
    @Override
    public void start() {
    }

    @Override
    public void stop() {
        broadcaster.close();
        running = false;
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        return PHASE;
    }
}
