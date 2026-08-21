package io.mohs.core;

import java.time.Instant;
import java.util.Objects;

/**
 * Um node do cluster visto pelo registro de heartbeat (ADR-0012/0041):
 * identidade, o estado auto-reportado no último tick e o last-seen. Morte
 * NÃO é um campo de propósito — crash não escreve nada; vivo/suspeito
 * deriva da idade de {@code lastHeartbeatAt} na leitura, {@code STOPPED}
 * é o único desfecho auto-reportado (shutdown limpo), e o purge da
 * ADR-0041 limita a lista a nodes recentes.
 */
public record NodeSnapshot(String nodeId, EngineState state, Instant lastHeartbeatAt) {

    public NodeSnapshot {
        Objects.requireNonNull(nodeId, "nodeId");
        Objects.requireNonNull(state, "state");
        Objects.requireNonNull(lastHeartbeatAt, "lastHeartbeatAt");
    }
}
