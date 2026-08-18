package io.mohs.core;

import java.time.Duration;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import io.mohs.core.execution.ExecutionState;

/**
 * A visão agregada de {@link Mohs#overview} — âncora de polling do
 * dashboard ({@code GET /overview}): contagens do trabalho vivo e vazão
 * terminal da janela recente. Estados terminais NÃO têm contagem all-time
 * de propósito: o histórico cresce sem teto e isto é consultado em
 * polling — o custo tem que ser proporcional ao trabalho vivo e à janela,
 * nunca ao tamanho da tabela ("barato por construção",
 * {@code docs/REST-API-DESIGN.md}); a atividade terminal recente aparece
 * em {@code succeededInWindow}/{@code failedInWindow}.
 *
 * <p>{@code executionCountsByState} sempre carrega os três estados vivos
 * ({@code ENQUEUED}, {@code RUNNING}, {@code RETRY_SCHEDULED}), com zero
 * quando não há linha — chave ausente e zero são a mesma informação, e um
 * contrato de polling não deve obrigar o consumidor a distinguir as duas.
 */
public record OverviewSnapshot(Map<ExecutionState, Long> executionCountsByState, Duration throughputWindow,
        long succeededInWindow, long failedInWindow) {

    /** Os estados que o mapa sempre carrega — o trabalho vivo. */
    private static final Set<ExecutionState> ACTIVE_STATES =
            Set.of(ExecutionState.ENQUEUED, ExecutionState.RUNNING, ExecutionState.RETRY_SCHEDULED);

    public OverviewSnapshot {
        Objects.requireNonNull(executionCountsByState, "executionCountsByState");
        Objects.requireNonNull(throughputWindow, "throughputWindow");
        if (throughputWindow.isNegative() || throughputWindow.isZero()) {
            throw new IllegalArgumentException("throughputWindow must be positive, got " + throughputWindow);
        }
        for (ExecutionState state : executionCountsByState.keySet()) {
            if (!ACTIVE_STATES.contains(state)) {
                throw new IllegalArgumentException("executionCountsByState only carries live work, got " + state
                        + " — all-time terminal counts are excluded by contract (unbounded history on a polling anchor)");
            }
        }
        // EnumMap + unmodifiableMap, não Map.copyOf: a ordem de declaração do
        // enum (ENQUEUED antes de RUNNING antes de RETRY_SCHEDULED) é a ordem
        // do ciclo de vida, e ordem exposta em API é contrato.
        Map<ExecutionState, Long> normalized = new EnumMap<>(ExecutionState.class);
        for (ExecutionState state : ACTIVE_STATES) {
            normalized.put(state, executionCountsByState.getOrDefault(state, 0L));
        }
        executionCountsByState = Collections.unmodifiableMap(normalized);
    }
}
