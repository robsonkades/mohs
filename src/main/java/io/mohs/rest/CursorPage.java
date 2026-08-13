package io.mohs.rest;

import java.util.List;
import java.util.Objects;

import org.jspecify.annotations.Nullable;

/**
 * Página de resultado por keyset pagination (PoEAA/DDIA) — {@code
 * nextCursor} ausente marca a última página. Usada só nas listagens
 * genuinamente sem teto ({@code GET /executions},
 * {@code GET /jobs/{jobKey}/executions}); listagens de cardinalidade
 * limitada (jobs, queues, rate-limits, runners, nodes) retornam
 * {@code List<T>} direto.
 */
public record CursorPage<T>(List<T> items, @Nullable String nextCursor) {

    public CursorPage {
        Objects.requireNonNull(items, "items");
        items = List.copyOf(items);
    }
}
