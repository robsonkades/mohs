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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CancellationSignalTest {

    /** The first reason wins — a TIMEOUT arriving after a MANUAL does not reclassify the outcome. */
    @Test
    void firstReasonWins() {
        CancellationSignal signal = new CancellationSignal();

        signal.requestCancellation(CancellationSignal.Reason.MANUAL, false);
        signal.requestCancellation(CancellationSignal.Reason.TIMEOUT, false);

        assertThat(signal.cancellationRequested()).isTrue();
        assertThat(signal.reason()).isEqualTo(CancellationSignal.Reason.MANUAL);
    }

    /** The interrupt reaches the registered thread even while it blocks on I/O or a wait — the real timeout case. */
    @Test
    void interruptIsDeliveredToTheRegisteredThread() throws Exception {
        CancellationSignal signal = new CancellationSignal();
        CountDownLatch registered = new CountDownLatch(1);
        CountDownLatch never = new CountDownLatch(1);
        AtomicBoolean interrupted = new AtomicBoolean();
        Thread handler = Thread.ofVirtual().name("mohs-job-test").start(() -> {
            signal.registerHandlerThread();
            registered.countDown();
            try {
                never.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                interrupted.set(true);
            } finally {
                signal.unregisterHandlerThreadAndClearInterrupt();
            }
        });

        assertThat(registered.await(5, TimeUnit.SECONDS)).isTrue();
        signal.requestCancellation(CancellationSignal.Reason.TIMEOUT, true);
        handler.join(Duration.ofSeconds(5));

        assertThat(interrupted).isTrue();
    }

    /**
     * After deregistration no interrupt is delivered AND the pending status has been cleared — a CPU
     * runner's platform thread returns clean to the pool, and the completion write (JDBC) never runs
     * interrupted.
     */
    @Test
    void unregisterStopsDeliveryAndClearsAPendingInterrupt() {
        CancellationSignal signal = new CancellationSignal();
        signal.registerHandlerThread();
        signal.requestCancellation(CancellationSignal.Reason.SHUTDOWN, true);
        assertThat(Thread.currentThread().isInterrupted()).isTrue();

        signal.unregisterHandlerThreadAndClearInterrupt();

        assertThat(Thread.currentThread().isInterrupted()).isFalse();
        // A NEW signal, to isolate the window: reusing the same object would prove interrupt
        // redelivery (which is another test's business), not the fact that outside the registration an
        // interrupt is delivered to nobody
        CancellationSignal reopened = new CancellationSignal();
        reopened.registerHandlerThread();
        reopened.unregisterHandlerThreadAndClearInterrupt();
        reopened.requestCancellation(CancellationSignal.Reason.SHUTDOWN, true);
        assertThat(Thread.currentThread().isInterrupted()).isFalse();
    }

    /** A signal raised before the handler starts (the task still queued in the executor) is only a flag — there is no thread to interrupt. */
    @Test
    void cancellationBeforeTheHandlerStartsIsFlagOnly() {
        CancellationSignal signal = new CancellationSignal();

        signal.requestCancellation(CancellationSignal.Reason.SHUTDOWN, true);

        assertThat(signal.cancellationRequested()).isTrue();
        assertThat(Thread.currentThread().isInterrupted()).isFalse();
    }

    /** The timeout clock only runs between registration and deregistration — a runner's queue does not count, and a finished handler does not fire. */
    @Test
    void handlerRunningLongerThanTracksOnlyTheRegisteredWindow() throws Exception {
        CancellationSignal signal = new CancellationSignal();
        assertThat(signal.handlerRunningLongerThan(Duration.ZERO)).isFalse();

        signal.registerHandlerThread();
        Thread.sleep(Duration.ofMillis(20));
        assertThat(signal.handlerRunningLongerThan(Duration.ofMillis(5))).isTrue();
        assertThat(signal.handlerRunningLongerThan(Duration.ofMinutes(5))).isFalse();

        signal.unregisterHandlerThreadAndClearInterrupt();
        assertThat(signal.handlerRunningLongerThan(Duration.ofMillis(5))).isFalse();
    }

    /**
     * The regression guard: the reason is first-wins, interrupt DELIVERY is not. A handler that caught
     * the timeout's interrupt, cleaned up and blocked again was immune to the shutdown's interrupt —
     * while the drain's log promised the opposite. Here the timeout arrives first, the handler
     * "consumes" the interrupt, and the shutdown must still be able to interrupt again.
     */
    @Test
    void shutdownStillInterruptsAHandlerThatSwallowedTheTimeoutInterrupt() {
        CancellationSignal signal = new CancellationSignal();
        signal.registerHandlerThread();

        signal.requestCancellation(CancellationSignal.Reason.TIMEOUT, true);
        assertThat(Thread.interrupted()).isTrue(); // o handler consome e limpa

        signal.requestCancellation(CancellationSignal.Reason.SHUTDOWN, true);

        assertThat(Thread.currentThread().isInterrupted())
                .as("the shutdown lever must not depend on who signalled first")
                .isTrue();
        assertThat(signal.reason())
                .as("the REASON stays first-wins: it decides the outcome mapping")
                .isEqualTo(CancellationSignal.Reason.TIMEOUT);
        Thread.interrupted();
        signal.unregisterHandlerThreadAndClearInterrupt();
    }
}
