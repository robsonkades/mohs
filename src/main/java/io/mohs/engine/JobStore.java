package io.mohs.engine;

import java.util.Optional;
import java.util.stream.Stream;

import io.mohs.core.definition.JobDefinition;
import io.mohs.core.job.JobKey;

/**
 * Persistência de {@link JobDefinition} — Repository (PoEAA), porta que
 * {@code io.mohs.jdbc} implementa (Data Mapper). {@link #upsert} segue a
 * precisão da ADR-0006: só grava estado definicional, nunca
 * {@code orphaned}/{@code paused} (operacional, exclusivo de
 * {@link #markOrphaned}/{@link #pause}/{@link #resume}).
 */
public interface JobStore {

    JobDefinition upsert(JobDefinition definition);

    Optional<StoredJob> find(JobKey key);

    /**
     * Stream sobre um cursor aberto — não materializa a tabela inteira em
     * memória de uma vez. Quem chama é dono do ciclo de vida
     * (try-with-resources); fechar o stream libera a conexão por trás.
     */
    Stream<StoredJob> findAll();

    /** {@code ANNOTATION} presente no store, ausente do código (ADR-0006) — não dispara, não apaga histórico. */
    void markOrphaned(JobKey key);

    void pause(JobKey key);

    void resume(JobKey key);

    /** Aposentadoria explícita ({@code Mohs#remove}) — só pra definições {@code PROGRAMMATIC}. */
    void remove(JobKey key);
}
