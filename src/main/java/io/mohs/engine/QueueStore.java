package io.mohs.engine;

import java.util.Optional;
import java.util.stream.Stream;

import io.mohs.core.resource.JobQueue;

/**
 * Persistência de {@link JobQueue} — Repository (PoEAA), porta que
 * {@code io.mohs.jdbc} implementa. {@link #upsert} só grava
 * {@code maxConcurrent} (definicional); {@code runningCount} (operacional,
 * ADR-0009) é exclusivo do claim (etapa 3) — mesma precisão que
 * {@link JobStore#upsert} já aplica a {@code orphaned}/{@code paused}.
 */
public interface QueueStore {

    JobQueue upsert(JobQueue queue);

    Optional<StoredQueue> find(String name);

    /** Stream sobre um cursor aberto — quem chama é dono do ciclo de vida (try-with-resources). */
    Stream<StoredQueue> findAll();

    /**
     * Reserva uma vaga se {@code runningCount < maxConcurrent} — incremento
     * atômico guardado, sem `SELECT` prévio (mesma disciplina de
     * {@code JdbcBatchStore#incrementSucceeded}, com a guarda a mais).
     * Chamado pelo claim (etapa 3), candidato a candidato, dentro da mesma
     * transação — não um `SELECT`/decide em memória, que reabriria a corrida
     * entre nós que a ADR-0017 resolve pro mutex por job.
     *
     * @return {@code true} se reservou a vaga; {@code false} se a queue já
     *         está no limite (candidato fica de fora deste batch de claim).
     */
    boolean tryIncrementRunning(String name);
}
