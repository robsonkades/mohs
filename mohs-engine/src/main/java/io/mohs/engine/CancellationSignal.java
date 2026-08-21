package io.mohs.engine;

import java.time.Duration;
import java.util.concurrent.locks.ReentrantLock;

import org.jspecify.annotations.Nullable;

/**
 * O sinal de cancelamento de UMA encarnação de dispatch (ADR-0034): flag
 * cooperativa + razão + entrega de interrupt à thread do handler. É o
 * ponto de encontro das três fontes — timeout do job ({@code TIMEOUT}),
 * estouro do grace de drain ({@code SHUTDOWN}) e
 * {@code POST /executions/{id}/cancel} ({@code MANUAL}) — e a fonte que
 * {@code JobContext.cancellationRequested()} lê.
 *
 * <p>Registro, interrupt e desregistro compartilham o mesmo lock (o
 * problema do {@code FutureTask.cancel}, JCIP cap. 7): o interrupt só é
 * entregue enquanto a thread está registrada, e
 * {@link #unregisterHandlerThreadAndClearInterrupt()} limpa o status
 * pendente DEPOIS de sair do registro — nenhum interrupt novo pode chegar,
 * então a escrita de conclusão (JDBC) nunca roda interrompida e a thread
 * de plataforma de um runner CPU nunca volta envenenada ao pool.
 * {@code ReentrantLock}, não {@code synchronized} — disciplina do projeto
 * em caminho concorrente (capacidades explícitas, JCIP cap. 13).
 */
final class CancellationSignal {

    /** A primeira razão registrada vence — ela decide o mapeamento de desfecho no {@code Dispatcher} (ADR-0034). */
    enum Reason { TIMEOUT, SHUTDOWN, MANUAL }

    private final ReentrantLock lock = new ReentrantLock();

    private volatile @Nullable Reason reason;
    /** Carimbo monotônico do início REAL do handler — fila de runner não conta (ADR-0034); lido pelo tick sem lock. */
    private volatile long handlerStartedNanos;
    private volatile boolean handlerStarted;
    private volatile boolean handlerFinished;

    /** Guardada pelo {@code lock}; {@code null} fora da janela registro→desregistro. */
    private @Nullable Thread handlerThread;

    boolean cancellationRequested() {
        return reason != null;
    }

    @Nullable Reason reason() {
        return reason;
    }

    /**
     * Levanta o sinal — a primeira razão vence, as seguintes são no-op.
     * {@code interrupt} entrega {@code Thread.interrupt()} se (e só se) a
     * thread do handler está registrada; antes do registro ou depois do
     * desregistro, vira flag pura.
     */
    void requestCancellation(Reason newReason, boolean interrupt) {
        lock.lock();
        try {
            if (reason != null) {
                return;
            }
            reason = newReason;
            if (interrupt && handlerThread != null) {
                handlerThread.interrupt();
            }
        } finally {
            lock.unlock();
        }
    }

    /** Chamado pela própria thread do handler, imediatamente antes de invocar a cadeia — abre a janela de interrupt e liga o relógio do timeout. */
    void registerHandlerThread() {
        lock.lock();
        try {
            handlerThread = Thread.currentThread();
            handlerStartedNanos = System.nanoTime();
            handlerStarted = true;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Fecha a janela de interrupt e limpa o status pendente — sempre em
     * {@code finally}, antes de qualquer escrita de conclusão. A limpeza
     * fica fora do lock de propósito: com o desregistro commitado, nenhum
     * {@link #requestCancellation} concorrente pode mais interromper, então
     * o {@code Thread.interrupted()} apaga tudo que possa ter chegado.
     */
    void unregisterHandlerThreadAndClearInterrupt() {
        lock.lock();
        try {
            handlerThread = null;
            handlerFinished = true;
        } finally {
            lock.unlock();
        }
        Thread.interrupted();
    }

    /**
     * O relógio do timeout: {@code true} se o handler começou, ainda não
     * terminou e roda há mais que {@code duration} — tempo monotônico
     * ({@code System.nanoTime}), nunca o {@code Clock} injetado (duração
     * não pode saltar no resync, invariante do projeto).
     */
    boolean handlerRunningLongerThan(Duration duration) {
        return handlerStarted && !handlerFinished
                && System.nanoTime() - handlerStartedNanos >= duration.toNanos();
    }
}
