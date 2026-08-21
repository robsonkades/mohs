package io.mohs.engine;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CancellationSignalTest {

    /** A primeira razão vence — TIMEOUT que chega depois de MANUAL não reclassifica o desfecho. */
    @Test
    void firstReasonWins() {
        CancellationSignal signal = new CancellationSignal();

        signal.requestCancellation(CancellationSignal.Reason.MANUAL, false);
        signal.requestCancellation(CancellationSignal.Reason.TIMEOUT, false);

        assertThat(signal.cancellationRequested()).isTrue();
        assertThat(signal.reason()).isEqualTo(CancellationSignal.Reason.MANUAL);
    }

    /** O interrupt alcança a thread registrada mesmo bloqueada em I/O/espera — o caso do timeout de verdade. */
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
     * Depois do desregistro nenhum interrupt é entregue E o status pendente
     * foi limpo — a thread de plataforma de um runner CPU volta limpa ao
     * pool, e a escrita de conclusão (JDBC) nunca roda interrompida.
     */
    @Test
    void unregisterStopsDeliveryAndClearsAPendingInterrupt() {
        CancellationSignal signal = new CancellationSignal();
        signal.registerHandlerThread();
        signal.requestCancellation(CancellationSignal.Reason.SHUTDOWN, true);
        assertThat(Thread.currentThread().isInterrupted()).isTrue();

        signal.unregisterHandlerThreadAndClearInterrupt();

        assertThat(Thread.currentThread().isInterrupted()).isFalse();
        // sinal NOVO: no mesmo objeto, a segunda requestCancellation pararia no
        // first-reason-wins antes de chegar ao interrupt — provaria o
        // short-circuit, não a janela fechada
        CancellationSignal reopened = new CancellationSignal();
        reopened.registerHandlerThread();
        reopened.unregisterHandlerThreadAndClearInterrupt();
        reopened.requestCancellation(CancellationSignal.Reason.SHUTDOWN, true);
        assertThat(Thread.currentThread().isInterrupted()).isFalse();
    }

    /** Sinal levantado antes do handler começar (task ainda na fila do executor) é só flag — não há thread pra interromper. */
    @Test
    void cancellationBeforeTheHandlerStartsIsFlagOnly() {
        CancellationSignal signal = new CancellationSignal();

        signal.requestCancellation(CancellationSignal.Reason.SHUTDOWN, true);

        assertThat(signal.cancellationRequested()).isTrue();
        assertThat(Thread.currentThread().isInterrupted()).isFalse();
    }

    /** O relógio do timeout só corre entre registro e desregistro — fila de runner não conta, handler terminado não dispara. */
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
}
