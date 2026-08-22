package io.mohs.engine;

import java.time.Instant;
import java.util.List;

import io.mohs.core.EngineState;
import io.mohs.core.execution.Execution;

/**
 * Registro de heartbeat de node (ADR-0012) — Repository (PoEAA), porta
 * que {@code io.mohs.store.jdbc} implementa. Só informativo: nenhuma lógica de
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

    /**
     * Remove heartbeats estritamente mais velhos que {@code cutoff}
     * (ADR-0041) — cada boot gera um {@code node_id} novo, então linha de
     * instância morta/reiniciada só sai por aqui; morte NÃO é escrita por
     * quem morreu (crash não avisa — ADR-0012 deriva "morto" da staleness
     * na leitura), o purge apenas recolhe o que já ficou ilegível de tão
     * velho.
     *
     * @return quantas linhas saíram
     */
    int deleteHeartbeatsBefore(Instant cutoff);
}
