package io.mohs.engine;

import java.time.Duration;
import java.util.Objects;

import org.jspecify.annotations.Nullable;

/**
 * Parâmetros de tempo e lote do {@link Engine} — snapshot imutável no
 * lugar de parâmetros posicionais de construtor que só cresciam (Long
 * Parameter List). {@code leaseTtl} é o mesmo valor que alimenta o claim
 * ({@code mohs.engine.lease-ttl}): o horizonte com que a lease de
 * EXECUÇÃO nasce no claim — e, desde a ADR-0051, também o corte de
 * staleness para linha de node legado sem {@code expires_at}.
 * {@code nodeLeaseTtl} ({@code mohs.engine.node-lease-ttl}, ADR-0051) é
 * a lease do NÓ: o heartbeat de cada tick promete "estou vivo até
 * agora+TTL" em {@code mohs_nodes.expires_at}, e o reaper só considera
 * morto o node cuja promessa venceu — a autoridade de liveness que
 * substituiu a renovação por execução.
 * {@code watchdogTimeout} é o teto opcional de runtime (Watchdog
 * Bound, ADR-0012, semântica revista pela ADR-0051: vencido o bound, o
 * node LIBERA a posse da execução em vez de só parar de renovar):
 * {@code null} = sem teto; quando presente, precisa ser maior que a
 * lease do nó — um bound menor liberaria posse antes de o node sequer
 * poder ser considerado morto. O bound mede
 * submit→agora em tempo monotônico: espera na fila de um runner CPU
 * conta como runtime — semântica deliberada até o interrupt por
 * timeout de job (próximo ciclo) trazer um carimbo do início real.
 * {@code misfireThreshold} (ADR-0035) separa disparo atrasado de disparo
 * perdido: ocorrência devida dentro do threshold dispara atrasada em
 * qualquer política; mais velha que ele responde ao {@code Misfire} do
 * job.
 *
 * <p>{@code dispatchConcurrency} (ADR-0039) é o teto de execuções em voo do
 * node — o mesmo valor que dimensiona o runner {@code io} built-in
 * ({@code mohs.engine.dispatch-concurrency}); o claim de cada tick é
 * limitado pela folga em relação a ele, para o node nunca reivindicar o
 * que não tem capacidade de despachar.
 *
 * <p>{@code claimRounds} (ADR-0040) é quantos claims um MESMO tick pode
 * encadear enquanto o lote voltar cheio e houver folga de dispatch —
 * relaxa o acoplamento da vazão com o {@code poll-interval} sob backlog
 * (medição na ADR-0040; o teto por ciclo continua sendo a folga de
 * dispatch); {@code 1} (default) preserva o formato clássico de um claim
 * por tick. Um lote que volta menor que o pedido encerra os rounds (a
 * fila drenou — o round seguinte seria um SELECT vazio).
 * Dimensionamento: o tick emite o heartbeat UMA vez, antes dos rounds —
 * por isso os rounds carregam um orçamento monotônico de
 * {@code nodeLeaseTtl/4} além do contador; sem ele, {@code claimRounds ×
 * latência-de-claim} perto do TTL faria um tick longo deixar a lease do
 * nó vencer no meio dos rounds e o reaper de outro node duplicar tudo
 * que estava em voo. O alongamento do tick também adia os sinais de
 * timeout/cancel (ADR-0034) — mais um motivo para rounds serem poucos.
 */
public record EngineSettings(Duration pollInterval, int batchSize, int dispatchConcurrency, int claimRounds,
        Duration leaseTtl, Duration nodeLeaseTtl, @Nullable Duration watchdogTimeout, Duration misfireThreshold) {

    /** Mesmo default de {@code mohs.engine.misfire-threshold} ({@code MohsProperties}) — precedente Quartz. */
    public static final Duration DEFAULT_MISFIRE_THRESHOLD = Duration.ofSeconds(60);

    /**
     * Claim sem teto de dispatch — o comportamento anterior à ADR-0039,
     * preservado pelos construtores de conveniência (uso de teste);
     * produção ({@code MohsAutoConfiguration}) sempre passa o teto real
     * pelo construtor canônico.
     */
    private static final int UNBOUNDED_DISPATCH = Integer.MAX_VALUE;

    public EngineSettings {
        Objects.requireNonNull(pollInterval, "pollInterval");
        Objects.requireNonNull(leaseTtl, "leaseTtl");
        Objects.requireNonNull(nodeLeaseTtl, "nodeLeaseTtl");
        Objects.requireNonNull(misfireThreshold, "misfireThreshold");
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be positive");
        }
        if (dispatchConcurrency <= 0) {
            throw new IllegalArgumentException("mohs.engine.dispatch-concurrency must be positive, got " + dispatchConcurrency);
        }
        if (claimRounds <= 0) {
            throw new IllegalArgumentException("mohs.engine.claim-rounds must be positive, got " + claimRounds
                    + " — 1 is the classic one-claim-per-tick shape, not zero");
        }
        if (!pollInterval.isPositive()) {
            throw new IllegalArgumentException("mohs.engine.poll-interval must be positive, got " + pollInterval);
        }
        if (!leaseTtl.isPositive()) {
            throw new IllegalArgumentException("mohs.engine.lease-ttl must be positive, got " + leaseTtl
                    + " — a non-positive lease is born expired and turns the first tick into a reclaim storm");
        }
        if (!nodeLeaseTtl.isPositive()) {
            throw new IllegalArgumentException("mohs.engine.node-lease-ttl must be positive, got " + nodeLeaseTtl
                    + " — a non-positive node lease is born expired and every peer's reaper reclaims this node's work");
        }
        if (watchdogTimeout != null && watchdogTimeout.compareTo(nodeLeaseTtl) <= 0) {
            throw new IllegalArgumentException("mohs.engine.watchdog-timeout (" + watchdogTimeout
                    + ") must be greater than mohs.engine.node-lease-ttl (" + nodeLeaseTtl
                    + ") — the bound is the ceiling ON TOP of node liveness (ADR-0051), not a shorter lease");
        }
        if (!misfireThreshold.isPositive()) {
            throw new IllegalArgumentException("mohs.engine.misfire-threshold must be positive, got " + misfireThreshold
                    + " — a non-positive threshold turns every normally-late fire into a misfire");
        }
    }

    /** Um claim por tick (pré-ADR-0040) e lease de nó = lease de execução — conveniência para quem só configura o teto de dispatch. */
    public EngineSettings(Duration pollInterval, int batchSize, int dispatchConcurrency, Duration leaseTtl,
            @Nullable Duration watchdogTimeout, Duration misfireThreshold) {
        this(pollInterval, batchSize, dispatchConcurrency, 1, leaseTtl, leaseTtl, watchdogTimeout, misfireThreshold);
    }

    /** Threshold de misfire default (ADR-0035), claim sem teto de dispatch (pré-ADR-0039) e um claim por tick — conveniência de teste. */
    public EngineSettings(Duration pollInterval, int batchSize, Duration leaseTtl, @Nullable Duration watchdogTimeout) {
        this(pollInterval, batchSize, UNBOUNDED_DISPATCH, leaseTtl, watchdogTimeout, DEFAULT_MISFIRE_THRESHOLD);
    }

    /** Sem Watchdog Bound (default da ADR-0012), threshold de misfire default (ADR-0035), claim sem teto de dispatch (pré-ADR-0039) e um claim por tick — conveniência de teste. */
    public EngineSettings(Duration pollInterval, int batchSize, Duration leaseTtl) {
        this(pollInterval, batchSize, leaseTtl, null);
    }
}
