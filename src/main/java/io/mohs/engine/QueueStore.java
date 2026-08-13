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
}
