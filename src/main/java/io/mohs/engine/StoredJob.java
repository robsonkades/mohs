package io.mohs.engine;

import java.util.Objects;

import io.mohs.core.definition.JobDefinition;

/**
 * {@link JobDefinition} mais o estado operacional que a ADR-0006
 * distingue do definicional — {@code orphaned}/{@code paused} não são
 * campos de {@link JobDefinition} de propósito (upsert nunca os toca);
 * este tipo só existe pra leitura combinada em {@link JobStore}.
 */
public record StoredJob(JobDefinition definition, boolean orphaned, boolean paused) {

    public StoredJob {
        Objects.requireNonNull(definition, "definition");
    }
}
