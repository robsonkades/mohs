package io.mohs.core;

import java.time.Instant;
import java.util.Objects;

import org.jspecify.annotations.Nullable;

import io.mohs.core.execution.Execution;
import io.mohs.core.execution.ExecutionState;
import io.mohs.core.job.JobKey;

/**
 * Filtro + paginação de {@link Mohs#executions} — parameter object
 * (Fowler, "Introduce Parameter Object") pros 6 filtros/controles de
 * {@code GET /executions} do design REST, todos opcionais exceto
 * {@code limit}. {@code cursor} é opaco: o {@code id} (UUIDv7, ordenável
 * cronologicamente) do último item da página anterior — resultado
 * ordenado por {@code id} decrescente (mais recente primeiro), só
 * {@link Execution}s com {@code id < cursor} quando presente.
 *
 * @param limit teto de itens devolvidos, pelo menos 1
 */
public record ExecutionQuery(
        @Nullable JobKey jobKey,
        @Nullable ExecutionState status,
        @Nullable Instant from,
        @Nullable Instant to,
        @Nullable String cursor,
        int limit) {

    public ExecutionQuery {
        if (limit < 1) {
            throw new IllegalArgumentException("limit must be at least 1");
        }
    }
}
