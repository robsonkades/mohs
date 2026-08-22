package io.mohs.engine;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mohs.core.execution.ExecutionId;
import io.mohs.engine.ExecutionStore.Completion;
import io.mohs.engine.ExecutionStore.CompletionRequest;

/**
 * Group commit da conclusão (ADR-0047; §7.6 do redesign, antecipado para o
 * schema atual): resultados de dispatch entram numa fila limitada e são
 * descarregados numa única transação de {@link ExecutionStore#completeAll}
 * — {@code flushSize} resultados ou {@code flushInterval} decorrido desde
 * o primeiro pendente, o que vier primeiro. Era isto ou um commit síncrono
 * por execução no topo do perfil de waits ({@code LWLock:WALWrite},
 * BASELINE "Tuning fim a fim").
 *
 * <p><b>Custo semântico, declarado:</b> a janela entre "handler terminou" e
 * "resultado durável" cresce de ~1 ms para ≤ {@code flushInterval}; crash
 * nessa janela re-executa até {@code flushSize} resultados a mais que os em
 * voo. O contrato já era at-least-once — muda a exposição a duplicata, não
 * a garantia (medida na BASELINE, E5). Quem precisa do comportamento antigo
 * liga {@code mohs.engine.completion-flush-on-every-result}.
 *
 * <p>Backpressure estrutural (§3.2 do redesign): fila cheia bloqueia o
 * {@code submit} na thread do handler — o dispatch segue em voo, o claim
 * enxerga a folga menor (ADR-0039) e o node para de reivindicar além do que
 * consegue persistir. Falha do flush em lote recai em conclusão individual
 * por resultado (mesma transação de sempre); falha individual deixa a
 * execução {@code RUNNING} para o reaper — nunca mata a thread do flusher
 * nem os vizinhos de lote (ADR-0031: sem back-off interno; o caminho de
 * recuperação é o de sempre).
 *
 * <p>Thread única de flush, virtual e nomeada; {@code close()} (Spring,
 * depois do stop do engine — {@code SmartLifecycle} para primeiro) drena o
 * que restou; {@code submit} depois do close conclui síncrono pelo caminho
 * antigo — zumbi que termina depois do shutdown não perde o resultado.
 */
public final class CompletionBatcher implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(CompletionBatcher.class);

    /** Espera do poll ocioso — só define a latência de percepção do close, não o gatilho de flush. */
    private static final Duration IDLE_POLL = Duration.ofMillis(50);

    private record Pending(CompletionRequest request, Consumer<Completion> onOutcome) {
    }

    private final ExecutionStore executionStore;
    private final JobStore jobStore;
    private final int flushSize;
    private final Duration flushInterval;
    private final LinkedBlockingQueue<Pending> queue;
    private final Thread flusher;
    private volatile boolean closed;

    public CompletionBatcher(ExecutionStore executionStore, JobStore jobStore, int flushSize, Duration flushInterval) {
        this.executionStore = Objects.requireNonNull(executionStore, "executionStore");
        this.jobStore = Objects.requireNonNull(jobStore, "jobStore");
        if (flushSize <= 0) {
            throw new IllegalArgumentException("flushSize must be positive, got " + flushSize);
        }
        if (!Objects.requireNonNull(flushInterval, "flushInterval").isPositive()) {
            throw new IllegalArgumentException("flushInterval must be positive, got " + flushInterval);
        }
        this.flushSize = flushSize;
        this.flushInterval = flushInterval;
        // 4× o lote: espaço pra fila absorver rajada sem bloquear à toa, teto
        // baixo o bastante pro backpressure chegar às threads de handler
        this.queue = new LinkedBlockingQueue<>(flushSize * 4);
        this.flusher = Thread.ofVirtual().name("mohs-completion-flusher").unstarted(this::flushLoop);
    }

    /** Idempotente — thread fora do construtor (JCIP §3.2: {@code this} não escapa antes de construído). */
    public void start() {
        if (flusher.getState() == Thread.State.NEW) {
            flusher.start();
        }
    }

    /**
     * Enfileira o resultado; {@code onOutcome} roda na thread do flusher com
     * o veredito do CAS, DEPOIS do commit do lote — mesma garantia
     * "publica só o que ficou durável" do caminho síncrono. Depois do
     * {@code close()}, conclui síncrono na própria thread chamadora.
     */
    public void submit(CompletionRequest request, Consumer<Completion> onOutcome) {
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(onOutcome, "onOutcome");
        if (closed) {
            onOutcome.accept(executionStore.complete(request, jobStore));
            return;
        }
        try {
            queue.put(new Pending(request, onOutcome));
        } catch (InterruptedException e) {
            // completa ANTES de re-armar a flag: com ela de pé o acquire do
            // JDBC lançaria (mesmo racional do flushLoop) e o fallback se
            // derrotaria; o status restaurado é para o CHAMADOR observar
            // depois (JCIP §7.1.3)
            onOutcome.accept(executionStore.complete(request, jobStore));
            Thread.currentThread().interrupt();
        }
    }

    /**
     * A guarda de {@code Throwable} em volta do {@code flush} e o
     * {@code finally} que degrada para o caminho síncrono são a política de
     * falha da thread de serviço (JCIP §7.3): sem eles, um {@code Error}
     * (OOME, listener com initializer quebrado) mataria o flusher em
     * silêncio com {@code closed} falso — a fila encheria, todo submit
     * bloquearia para sempre e as execuções em voo ficariam presas com a
     * lease sendo renovada, fora do alcance do reaper.
     */
    private void flushLoop() {
        List<Pending> buffer = new ArrayList<>(flushSize);
        try {
            while (!(closed && queue.isEmpty())) {
                try {
                    Pending first = queue.poll(IDLE_POLL.toNanos(), TimeUnit.NANOSECONDS);
                    if (first == null) {
                        continue;
                    }
                    buffer.add(first);
                    // acumula em fatias de no máximo IDLE_POLL, não numa espera
                    // única de flushInterval: o close precisa ser observado no
                    // meio da janela — um intervalo longo (teste, configuração
                    // futura) travaria o dreno do shutdown pela janela inteira
                    long deadlineNanos = System.nanoTime() + flushInterval.toNanos();
                    while (buffer.size() < flushSize && !closed) {
                        long remainingNanos = deadlineNanos - System.nanoTime();
                        if (remainingNanos <= 0) {
                            break;
                        }
                        Pending next = queue.poll(Math.min(remainingNanos, IDLE_POLL.toNanos()), TimeUnit.NANOSECONDS);
                        if (next != null) {
                            buffer.add(next);
                        }
                    }
                } catch (InterruptedException e) {
                    // thread própria e dedicada: interrupt aqui só pode significar
                    // "encerre" — vira o protocolo de close (flush do buffer +
                    // dreno + saída). NÃO re-arma a flag: ela envenenaria o JDBC
                    // do flush (Hikari acquire lança com a flag de pé) e a
                    // retomada do poll viraria busy-spin.
                    closed = true;
                }
                try {
                    flush(buffer);
                } catch (Throwable t) {
                    log.error("completion flush cycle failed unexpectedly — {} result(s) fall to the reaper on lease expiry",
                            buffer.size(), t);
                }
                buffer.clear();
            }
        } finally {
            // se esta thread sair por QUALQUER via, submit degrada pro
            // caminho síncrono em vez de bloquear numa fila morta
            closed = true;
        }
    }

    /**
     * Uma transação para o lote inteiro; se ELA falhar, recai na conclusão
     * individual — resultado nunca é descartado por culpa dos vizinhos.
     * {@code onOutcome} guardado por resultado: um listener que lança não
     * derruba o flusher nem cala os irmãos de lote.
     */
    private void flush(List<Pending> buffer) {
        if (buffer.isEmpty()) {
            return;
        }
        Map<ExecutionId, Completion> verdicts;
        try {
            verdicts = executionStore.completeAll(buffer.stream().map(Pending::request).toList(), jobStore);
        } catch (RuntimeException e) {
            log.warn("group completion flush of {} result(s) failed — falling back to one completion per result", buffer.size(), e);
            for (Pending pending : buffer) {
                completeIndividually(pending);
            }
            return;
        }
        for (Pending pending : buffer) {
            deliverOutcome(pending, verdicts.getOrDefault(pending.request().id(), Completion.NOT_APPLIED));
        }
    }

    private void completeIndividually(Pending pending) {
        try {
            deliverOutcome(pending, executionStore.complete(pending.request(), jobStore));
        } catch (RuntimeException e) {
            log.error("could not record the completion of execution {} — it will sit RUNNING until the reaper reclaims it on lease expiry",
                    pending.request().id(), e);
        }
    }

    private static void deliverOutcome(Pending pending, Completion completion) {
        try {
            pending.onOutcome().accept(completion);
        } catch (RuntimeException e) {
            log.error("completion outcome callback of execution {} threw — the result IS durable, only its follow-up (events/metrics) was lost",
                    pending.request().id(), e);
        }
    }

    /**
     * Drena e para: espera o flusher (até 10s) e então varre a fila
     * sincronamente. A varredura final não é redundância — um {@code put}
     * que passou pelo check de {@code closed} no {@code submit} pode
     * aterrissar DEPOIS da saída do flusher (check-then-act não é atômico,
     * JCIP §2.2), e ela é quem apanha esse retardatário; também cobre
     * flusher nunca iniciado e o estouro do join. Chamado pelo Spring
     * depois do stop do engine.
     */
    @Override
    public void close() {
        closed = true;
        if (flusher.getState() != Thread.State.NEW) {
            try {
                if (!flusher.join(Duration.ofSeconds(10))) {
                    log.warn("completion flusher did not drain within 10s — draining the remaining {} result(s) synchronously",
                            queue.size());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        for (Pending pending; (pending = queue.poll()) != null; ) {
            completeIndividually(pending);
        }
    }
}
