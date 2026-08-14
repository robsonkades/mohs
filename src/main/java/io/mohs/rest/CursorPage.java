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
 *
 * <p>Tamanho de página: parâmetro {@code size} nos dois endpoints acima,
 * opcional — {@link #DEFAULT_PAGE_SIZE} quando ausente, {@link
 * #MAX_PAGE_SIZE} como teto rígido (requisitar mais que isso não estoura
 * erro, satura no teto) — decidido agora, no contrato M2, porque página
 * ilimitada numa tabela sem teto é superfície real de DoS; enforcement
 * fica pra M3 (`io.mohs.rest` ainda não tem implementação por trás).
 */
public record CursorPage<T>(List<T> items, @Nullable String nextCursor) {

    public static final int DEFAULT_PAGE_SIZE = 50;
    public static final int MAX_PAGE_SIZE = 200;

    public CursorPage {
        Objects.requireNonNull(items, "items");
        items = List.copyOf(items);
    }
}
