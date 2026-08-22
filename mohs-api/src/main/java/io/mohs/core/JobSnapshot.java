package io.mohs.core;

import java.time.Instant;
import java.util.Objects;

import org.jspecify.annotations.Nullable;

import io.mohs.core.definition.JobDefinition;

/**
 * {@link JobDefinition} mais o estado operacional que ela não carrega de
 * propósito ({@code paused}) e o próximo disparo calculado
 * ({@code nextFireAt}) — a forma de leitura devolvida por
 * {@link Mohs#findJob}/{@link Mohs#jobs}. Espelho público de
 * {@code io.mohs.engine.StoredJob}, sem {@code orphaned}/
 * {@code runningExecutionCount} (operacionais demais pra expor via API
 * ainda, sem consumidor que precise deles fora do motor).
 *
 * @param nextFireAt {@code null} pra {@code OnDemandSpec} (nunca dispara
 * sozinho) ou quando o job está pausado
 */
public record JobSnapshot(JobDefinition definition, boolean paused, @Nullable Instant nextFireAt) {

    public JobSnapshot {
        Objects.requireNonNull(definition, "definition");
    }
}
