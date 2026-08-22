package io.mohs.engine;

import java.time.Instant;
import java.util.List;

import io.mohs.core.EngineState;
import io.mohs.core.execution.Execution;

/**
 * Registro de heartbeat e lease de node (ADR-0012, promovida a
 * autoridade de liveness pela ADR-0051) — Repository (PoEAA), porta que
 * {@code io.mohs.store.jdbc} implementa. Deixou de ser só informativa: o
 * reaper decide "morto" pelo {@code expires_at} desta tabela (uma escrita
 * por node por tick), no lugar da renovação de lease POR EXECUÇÃO que
 * pagava ~5 updates/execução na tabela mais quente do sistema (Finding A
 * do redesign, medido na BASELINE da Phase 4).
 */
public interface NodeStore {

    /**
     * Registra o heartbeat mais recente do node — upsert por
     * {@code nodeId}. Desde a ADR-0051 o heartbeat carrega o lease do NÓ:
     * {@code epoch} (encarnação) e {@code expiresAt} (a promessa "estou
     * vivo até aqui" que o reaper consulta — a autoridade de liveness que
     * substituiu a renovação por execução).
     */
    void heartbeat(String nodeId, EngineState state, long epoch, Instant at, Instant expiresAt);

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
