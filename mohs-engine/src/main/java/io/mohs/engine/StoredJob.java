package io.mohs.engine;

import java.time.Instant;
import java.util.Objects;

import org.jspecify.annotations.Nullable;

import io.mohs.core.definition.JobDefinition;

/**
 * {@link JobDefinition} mais o estado operacional que a ADR-0006
 * distingue do definicional — {@code orphaned}/{@code paused} não são
 * campos de {@link JobDefinition} de propósito (upsert nunca os toca);
 * este tipo só existe pra leitura combinada em {@link JobStore}.
 * {@code nextFireAt} é o estado do trigger (ADR-0035): {@code null} =
 * nada a disparar — on-demand, ou fixed-delay aguardando o fim da
 * execução anterior. O contador de mutex por job da era ADR-0018/0020
 * não existe mais: desde a ADR-D o cap deriva de {@code mohs_lease}
 * ({@link LeaseStore#countByJob}) — posse viva É a vaga ocupada.
 */
public record StoredJob(JobDefinition definition, boolean orphaned, boolean paused,
        @Nullable Instant nextFireAt) {

    public StoredJob {
        Objects.requireNonNull(definition, "definition");
    }
}
