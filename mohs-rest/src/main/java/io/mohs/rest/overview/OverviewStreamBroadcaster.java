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

import org.jspecify.annotations.Nullable;
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

/**
 * O snapshot stream de {@code GET /overview/stream} — polling movido pro
 * servidor, não entrega de eventos: cada tick emite o estado corrente
 * completo em eventos SSE nomeados ({@code overview}, {@code jobs},
 * {@code nodes}, {@code executions}), cada um envelopado em
 * {@link SnapshotEnvelope} ({@code {asOf, data}}) — o {@code asOf} é o
 * instante do retrato, um só pros 4 eventos do tick. Desconexão não
 * perde nada — o próximo frame é o retrato inteiro de novo. É por isso que este
 * endpoint NÃO viola a decisão v0.3 do design REST ("sem SSE na v1"):
 * aquela decisão rejeitou entrega de eventos best-effort sem a futura
 * tabela durável; um snapshot periódico não promete durabilidade nenhuma
 * (revisão registrada no doc v0.7).
 *
 * <p>Um tick compartilhado por todos os assinantes (Observer, GoF): as
 * LEITURAS custam o mesmo que UM {@code GET /overview} + as listas por
 * intervalo, independente de quantos dashboards estão conectados — e
 * zero sem assinante (o tick sai antes de tocar o banco). As leituras
 * herdam o que as contagens fizerem — que desde a Phase 5 já não é o
 * contrato sem lock: elas foram reescritas sobre as tabelas do split e
 * deixaram de usar {@code JdbcDialect#lockFreeReadHint}, então em SQL
 * Server sem RCSI voltaram a tomar shared locks (pendência no PLAN.md).
 *
 * <p>Os ENVIOS são por assinante, em virtual thread (I/O de rede
 * bloqueante — regra da casa), com <em>conflation</em>: se o frame
 * anterior de um cliente ainda não terminou de escrever (cliente lento,
 * laptop suspenso com a conexão viva), o tick PULA esse cliente — fila
 * aqui seria contra o próprio design, o próximo frame é o retrato
 * completo de novo. Sem isso, a escrita servlet bloqueante do cliente
 * mais lento definiria a latência de todos (JCIP cap. 8: tarefa sem
 * bound de tempo monopolizando executor de cardinalidade fixa) e um
 * send travado seguraria o {@code writeLock} que o shutdown disputa.
 */
public final class OverviewStreamBroadcaster implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(OverviewStreamBroadcaster.class);

    /** Cadência fixa da v1, como {@link OverviewController#DEFAULT_THROUGHPUT_WINDOW} — vira propriedade quando alguém precisar de outra. */
    static final Duration STREAM_INTERVAL = Duration.ofSeconds(2);

    /** Teto da quiescência do escopo de leitura ({@code closeScope}) — sob falha, o retrato dura no máximo {@code STREAM_INTERVAL + FRAME_CANCEL_GRACE}. */
    static final Duration FRAME_CANCEL_GRACE = Duration.ofSeconds(1);

    private final Mohs mohs;
    private final Clock clock;
    private final CopyOnWriteArrayList<Subscriber> subscribers = new CopyOnWriteArrayList<>();
    private final ScheduledExecutorService scheduler;
    private final ExecutorService senders = Executors.newThreadPerTaskExecutor(
            Thread.ofVirtual().name("mohs-overview-sse-send-", 0).factory());

    /** Visível pro pacote por causa dos testes: constrói SEM timer — {@code tick()} direto, determinismo total (sem sleep, regra da casa). */
    OverviewStreamBroadcaster(Mohs mohs, Clock clock) {
        this.mohs = Objects.requireNonNull(mohs, "mohs");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.scheduler = Executors.newSingleThreadScheduledExecutor(
                Thread.ofPlatform().name("mohs-overview-sse").daemon().factory());
    }

    /**
     * Fábrica que liga o timer (Effective Java, Item 1): o agendamento fora
     * do construtor elimina o this-escape (JCIP §3.2) — hoje ele seria
     * seguro (submit estabelece happens-before), mas qualquer campo futuro
     * atribuído depois do agendamento seria publicado sem sincronização
     * pra thread do tick. Fixed-delay, não fixed-rate: um tick lento
     * (banco degradado) não acumula fila de ticks disparando em rajada.
     */
    public static OverviewStreamBroadcaster start(Mohs mohs, Clock clock) {
        OverviewStreamBroadcaster broadcaster = new OverviewStreamBroadcaster(mohs, clock);
        broadcaster.scheduler.scheduleWithFixedDelay(broadcaster::guardedTick,
                STREAM_INTERVAL.toMillis(), STREAM_INTERVAL.toMillis(), TimeUnit.MILLISECONDS);
        return broadcaster;
    }

    /**
     * Registra o cliente e envia o retrato inicial na hora — o dashboard
     * pinta sem esperar o primeiro tick. O envio inicial é síncrono de
     * propósito: antes do Spring inicializar o emitter, {@code send} só
     * bufferiza em memória — não há escrita de rede pra bloquear. Sem
     * timeout ({@code 0L}): o stream vive até o cliente desconectar; os
     * callbacks removem o assinante nos dois desfechos possíveis.
     */
    public SseEmitter subscribe() {
        SseEmitter emitter = new SseEmitter(0L);
        Subscriber subscriber = new Subscriber(emitter);
        emitter.onCompletion(() -> subscribers.remove(subscriber));
        emitter.onError(_ -> subscribers.remove(subscriber));
        // retrato ANTES de registrar: se a leitura falhar (banco degradado),
        // a exceção vira o 500 do subscribe sem deixar assinante órfão na
        // lista — um emitter nunca inicializado não dispara callback nenhum
        // e bufferizaria os sends de todo tick futuro sem teto.
        List<Frame> initialSnapshot = buildFrames();
        subscribers.add(subscriber);
        send(subscriber, initialSnapshot);
        return emitter;
    }

    /**
     * Package-private pra teste determinístico — o agendamento só embrulha
     * isto. Síncrono por construção: fixed-delay + quiescência do escopo de
     * leitura garantem que um tick nunca sobrepõe outro — sob banco
     * degradado o churn fica em 4 conexões por ~{@code STREAM_INTERVAL +
     * FRAME_CANCEL_GRACE}, ordens de magnitude abaixo do que o próprio
     * poll do engine faz sob a mesma degradação; a camada certa pra
     * timeout mais fino é {@code Statement.setQueryTimeout} no store (M3).
     */
    void tick() {
        if (subscribers.isEmpty()) {
            return;
        }
        List<Frame> frames = buildFrames();
        for (Subscriber subscriber : subscribers) {
            // conflation: frame anterior ainda em voo = cliente lento — pula;
            // ele atrasa só a si mesmo, e o próximo frame é o retrato inteiro
            if (subscriber.inFlight().compareAndSet(false, true)) {
                try {
                    senders.submit(() -> {
                        try {
                            send(subscriber, frames);
                        } finally {
                            subscriber.inFlight().set(false);
                        }
                    });
                } catch (RejectedExecutionException shuttingDown) {
                    // close() em curso: devolve a flag — sem isto, se o
                    // lifecycle um dia permitir tick pós-close, o assinante
                    // ficaria mudo pra sempre (starvation silenciosa)
                    subscriber.inFlight().set(false);
                }
            }
        }
    }

    /** Falha de leitura não pode matar o agendamento ({@code ScheduledExecutorService} cancela a tarefa que lança) — loga e tenta no próximo tick. */
    private void guardedTick() {
        try {
            tick();
        } catch (RuntimeException e) {
            log.warn("overview stream tick failed — clients keep their connection, retrying on the next tick ({})",
                    STREAM_INTERVAL, e);
        }
    }

    /**
     * O retrato corrente, um evento nomeado por tipo — cliente assina só o
     * que consome ({@code EventSource.addEventListener}). O frame de
     * {@code executions} usa {@link CursorPage#DEFAULT_PAGE_SIZE} de
     * propósito: o painel do dashboard mostra o MESMO recorte que a
     * primeira página de {@code GET /executions} — se um dia divergirem,
     * a divergência deve ser uma decisão, não um acidente.
     *
     * <p>As 4 leituras são independentes — fan-out estrutural na forma da
     * JEP 505 (fork das subtarefas, join com deadline, falha de uma
     * cancela as irmãs), mas com a API estável: {@code StructuredTaskScope}
     * é preview no JDK 25 e {@code --enable-preview} travaria o host da
     * biblioteca num JDK exato (regra do projeto — migração mecânica
     * quando finalizar). Latência do retrato = a leitura mais lenta, não a
     * soma; e o deadline de um {@link #STREAM_INTERVAL} (retrato que não
     * fecha dentro de um tick já nasce velho) garante que uma leitura
     * pendurada nunca congela o timer pra sempre.
     *
     * <p>O escopo é um executor POR CHAMADA, fechado no {@code finally}
     * com quiescência bounded ({@link #FRAME_CANCEL_GRACE}) — a garantia
     * central do join estrutural: NENHUMA subtarefa sobrevive ao escopo
     * (o review deste ciclo pegou a versão sem espera: {@code cancel(true)}
     * retorna na hora com a thread irmã ainda lendo o banco — cancelamento
     * é protocolo de duas fases, sinalizar E aguardar, JCIP §7.1.5). Sob
     * falha, o pior caso do retrato passa a ser
     * {@code STREAM_INTERVAL + FRAME_CANCEL_GRACE}.
     */
    private List<Frame> buildFrames() {
        long deadlineNanos = System.nanoTime() + STREAM_INTERVAL.toNanos();
        // um asOf por retrato, do Clock injetado (invariante da casa): os 4
        // eventos do mesmo tick carregam o MESMO instante — é o carimbo que
        // permite ao frontend ordenar/descartar frame atrasado; divergir
        // entre eventos do mesmo retrato só criaria falsa precedência.
        Instant asOf = clock.instant();
        ExecutorService scope = Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("mohs-overview-frame-", 0).factory());
        try {
            List<Future<Frame>> forks = List.of(
                    fork(scope, "overview", asOf, () -> OverviewResponse.from(mohs.overview(OverviewController.DEFAULT_THROUGHPUT_WINDOW))),
                    fork(scope, "jobs", asOf, () -> mohs.jobs().stream().map(JobResponse::from).toList()),
                    fork(scope, "nodes", asOf, () -> mohs.nodes().stream().map(NodeResponse::from).toList()),
                    fork(scope, "executions", asOf, () -> mohs
                            .executions(new ExecutionQuery(null, null, null, null, null, CursorPage.DEFAULT_PAGE_SIZE))
                            .stream().map(ExecutionSummaryResponse::from).toList()));

            List<Frame> frames = new ArrayList<>(forks.size());

            for (Future<Frame> fork : forks) {
                frames.add(fork.get(deadlineNanos - System.nanoTime(), TimeUnit.NANOSECONDS));
            }

            return List.copyOf(frames);
        } catch (ExecutionException e) {
            // launderThrowable (JCIP §5.5.2): exceção original preservada
            // quando dá — o RestExceptionHandler mapeia por tipo e o embrulho
            // de Future não é contrato de ninguém; Error nunca vira "estado
            // ilegal".
            switch (e.getCause()) {
                case RuntimeException runtime -> throw runtime;
                case Error error -> throw error;
                // null teórico (FutureTask sempre embrulha causa não-nula) —
                // mas pattern switch lança NPE em seletor nulo, e a guarda custa zero
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

    /** Uma subtarefa do fan-out: a leitura roda numa virtual thread do escopo DESTA chamada e o resultado já nasce como evento nomeado, envelopado com o {@code asOf} do retrato. */
    private static Future<Frame> fork(ExecutorService scope, String eventName, Instant asOf, Supplier<Object> read) {
        return scope.submit(() -> new Frame(eventName, new SnapshotEnvelope(asOf, read.get())));
    }

    /**
     * O fim do escopo, em qualquer desfecho: interrompe o que resta
     * ({@code shutdownNow}) e ESPERA a quiescência com teto — no caminho
     * feliz não resta ninguém e o retorno é imediato. Reader que ignorar a
     * interrupção além do grace (H2 embedded, sem socket, pode não honrar
     * interrupt) vira WARN de thread vazada, nunca espera infinita — o
     * timer continua protegido.
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
     * Escreve os frames num assinante; falha o remove na hora sem derrubar
     * os demais. Os dois desfechos têm severidades diferentes de propósito:
     * {@code IOException} é o fim normal de um SSE (cliente fechou — churn,
     * debug); {@code RuntimeException} é defeito NOSSO (ex.: serialização
     * de um DTO novo) e derrubaria cliente a cliente — precisa de WARN com
     * a causa, ou vira falha indiagnosticável às 3h.
     */
    private void send(Subscriber subscriber, List<Frame> frames) {
        try {
            for (Frame frame : frames) {
                subscriber
                        .emitter()
                        .send(SseEmitter.event().name(frame.name()).data(frame.data(), MediaType.APPLICATION_JSON));
            }
        } catch (IOException clientGone) {
            log.debug("SSE client dropped during send", clientGone);
            subscribers.remove(subscriber);
            completeQuietly(subscriber.emitter(), clientGone);
        } catch (RuntimeException serverSideFailure) {
            log.warn("dropping SSE subscriber after server-side send failure", serverSideFailure);
            subscribers.remove(subscriber);
            completeQuietly(subscriber.emitter(), serverSideFailure);
        }
    }

    /** Um emitter pode ter chegado ao próprio desfecho concorrentemente (desconexão no mesmo instante) — o segundo desfecho não pode custar o dos demais. */
    private static void completeQuietly(SseEmitter emitter, @Nullable Throwable cause) {
        try {
            if (cause == null) {
                emitter.complete();
            } else {
                emitter.completeWithError(cause);
            }
        } catch (RuntimeException alreadyClosed) {
            log.debug("emitter already closed while being discarded", alreadyClosed);
        }
    }

    /**
     * Shutdown limpo: para o timer e completa os streams abertos — o
     * cliente vê fim de stream, não conexão morta. Os completes vão pelas
     * virtual threads de envio: um cliente travado segurando o
     * {@code writeLock} do próprio emitter atrasa só o dele, nunca o
     * destroy do contexto. Sem guarda contra {@code subscribe} concorrente
     * de propósito: o Boot para o web server (sem novas requests) ANTES de
     * destruir os singletons — se o broadcaster um dia sair do lifecycle
     * do Spring, essa suposição cai e uma flag {@code closed} passa a ser
     * necessária.
     */
    @Override
    public void close() {
        scheduler.shutdownNow();
        for (Subscriber subscriber : subscribers) {
            senders.submit(() -> completeQuietly(subscriber.emitter(), null));
        }
        subscribers.clear();
        senders.shutdown();
    }

    private record Frame(String name, SnapshotEnvelope data) {
    }

    /**
     * O envelope de wire de todo evento do stream: {@code asOf} é o
     * instante do retrato (um só pros 4 eventos do mesmo tick) — o
     * frontend usa pra ordenar frames, descartar atrasado e distribuir
     * atualizações internamente sem depender do relógio do cliente.
     */
    record SnapshotEnvelope(Instant asOf, Object data) {
    }

    /** Emitter + a flag de conflation — {@code inFlight} é o CAS que garante no máximo um envio em voo por cliente. */
    private record Subscriber(SseEmitter emitter, AtomicBoolean inFlight) {

        Subscriber(SseEmitter emitter) {
            this(emitter, new AtomicBoolean(false));
        }
    }
}
