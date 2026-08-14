package io.mohs.engine;

import java.time.Instant;
import java.util.List;

import io.mohs.core.EngineState;
import io.mohs.core.execution.Execution;

/**
 * Registro de heartbeat de node (ADR-0012) — Repository (PoEAA), porta
 * que {@code io.mohs.jdbc} implementa. Só informativo: nenhuma lógica de
 * claim/reclaim consulta esta porta, é o relógio "cluster-wide" separado
 * do lease funcional de {@link Execution} que a ADR-0012 distingue.
 */
public interface NodeStore {

    /** Registra o heartbeat mais recente do node — upsert por {@code nodeId}. */
    void heartbeat(String nodeId, EngineState state, Instant at);

    /**
     * Todos os nodes já registrados, sem filtro de "recente" — o limiar de
     * staleness depende de configuração que ainda não existe
     * ({@code mohs.engine.node-heartbeat-interval}, ADR-0012 deixa em
     * aberto); {@code List}, não {@code Stream}: o tamanho desta tabela é
     * limitado pelo tamanho do cluster, não cresce sem limite como {@link
     * Execution}.
     */
    List<StoredNode> findAll();
}
