package io.mohs.engine;

import java.time.Duration;
import java.time.Instant;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import io.mohs.core.execution.Attempt;
import io.mohs.core.execution.ExecutionState;
import io.mohs.core.job.JobKey;

/**
 * As métricas {@code mohs.*} do engine (ARCHITECTURE_REDESIGN_PLAN §14.4,
 * Phase 0) — operabilidade é requisito, não acabamento (CLAUDE.md, "design
 * for 3 a.m."). Regra de cardinalidade aplicada aqui, não por convenção:
 * labels são {@code job} (limitado pelo número de definições),
 * {@code outcome}/{@code reason} (enums) — id de execução NUNCA vira label.
 *
 * <p>A duração de execução deriva dos timestamps do {@link Attempt} — a
 * mesma janela persistida que BASELINE e dashboard leem, uma fonte só. A
 * latência de claim chega em nanos medidos por {@code System.nanoTime} no
 * chamador (invariante de tempo monotônico do CLAUDE.md). Num resync de
 * relógio para trás a janela pode sair negativa e o {@code Timer} do
 * Micrometer descarta a amostra em silêncio — sumiço de amostras durante
 * resync é comportamento esperado, não perda de dado.
 *
 * <p>Valores de label são contrato tanto quanto os nomes: minúsculos,
 * snake_case ({@code succeeded}, {@code attempts_exhausted}) — o primeiro
 * dashboard salvo congela esse vocabulário.
 */
public final class EngineMetrics {

    private final MeterRegistry registry;
    private final Timer claimLatency;
    private final DistributionSummary claimBatchSize;

    public EngineMetrics(MeterRegistry registry) {
        this.registry = Objects.requireNonNull(registry, "registry");
        this.claimLatency = Timer.builder("mohs.claim.latency")
                .description("duration of one claim round against the store")
                .register(registry);
        this.claimBatchSize = DistributionSummary.builder("mohs.claim.batch.size")
                .description("executions claimed per round — full batches mean claim-bound, small ones dispatch-bound")
                .register(registry);
    }

    void claimRound(long elapsedNanos, int claimed) {
        claimLatency.record(elapsedNanos, TimeUnit.NANOSECONDS);
        claimBatchSize.record(claimed);
    }

    /** Do agendado ao início do handler — o SLO visível pro usuário, por job. */
    void dispatchLatency(JobKey job, Duration sinceScheduled) {
        registry.timer("mohs.dispatch.latency", "job", job.value()).record(sinceScheduled);
    }

    /**
     * Um attempt confirmado pelo CAS de conclusão. Todo attempt conta em
     * {@code mohs.attempt.total}; só transição terminal conta em
     * {@code mohs.execution.total} — retry ainda não é desfecho da
     * execução, e a razão attempts/executions é o indicador de saúde que
     * o §14.4 promete.
     */
    void attemptFinished(JobKey job, Attempt attempt, ExecutionState newState) {
        String outcome = labelValue(attempt.outcome());
        registry.counter("mohs.attempt.total", "job", job.value(), "outcome", outcome).increment();
        Instant finishedAt = attempt.finishedAt();
        if (finishedAt != null) {
            registry.timer("mohs.execution.duration", "job", job.value(), "outcome", outcome)
                    .record(Duration.between(attempt.startedAt(), finishedAt));
        }
        if (newState != ExecutionState.RETRY_SCHEDULED) {
            registry.counter("mohs.execution.total", "job", job.value(), "outcome", labelValue(newState)).increment();
        }
    }

    /**
     * Qualquer valor não-zero aqui significa node morto ou parado (§14.4) —
     * o label diz o que o reclaim decidiu. {@code attemptsExhausted} separa
     * o FAILED por orçamento esgotado do FAILED por job aposentado (o
     * Javadoc de {@link Reaper.Reclaimed} explica por que a Execution
     * sozinha não distingue) — confundi-los mandaria o operador investigar
     * retry budget num retirement em massa.
     */
    void leaseReclaimed(ExecutionState postReclaimState, boolean attemptsExhausted) {
        String reason = switch (postReclaimState) {
            case RETRY_SCHEDULED -> "retry";
            case FAILED -> attemptsExhausted ? "attempts_exhausted" : "job_retired";
            case CANCELLED -> "cancelled";
            // inalcançáveis pós-reclaim; braços explícitos para o compilador acusar um ExecutionState novo
            case ENQUEUED, RUNNING, SUCCEEDED -> labelValue(postReclaimState);
        };
        registry.counter("mohs.lease.reclaimed", "reason", reason).increment();
    }

    private static String labelValue(ExecutionState state) {
        return state.name().toLowerCase(Locale.ROOT);
    }

    /**
     * {@code strongReference}: o supplier é uma method reference efêmera —
     * sem ela o gauge sumiria no primeiro GC. Limitação registrada: sem o
     * label {@code node}, dois engines no mesmo registry (um Mohs por
     * datasource, §13.3 do plano) colidem no id e o segundo bind é ignorado
     * em silêncio pelo Micrometer — o gatilho para adicionar a tag é o
     * primeiro cenário multi-engine real, pagando a cardinalidade só então.
     */
    void bindNodeGauges(Supplier<Number> inFlight, int capacity) {
        Gauge.builder("mohs.node.inflight", inFlight)
                .description("executions currently dispatched on this node")
                .strongReference(true)
                .register(registry);
        Gauge.builder("mohs.node.capacity", () -> capacity)
                .description("dispatch-concurrency of this node")
                .strongReference(true)
                .register(registry);
    }
}
