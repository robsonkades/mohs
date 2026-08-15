package io.mohs.rest;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;

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

    /**
     * Monta a página a partir de um resultado buscado com {@code size + 1}
     * itens (convenção de {@code ExecutionStore#findPage}) — o item extra
     * denuncia se há próxima página sem round-trip a mais, e é descartado
     * do corpo devolvido.
     *
     * @param fetched no máximo {@code size + 1} itens
     * @param size tamanho de página pedido pelo chamador
     * @param cursorOf extrai o cursor opaco do último item de uma página cheia
     */
    public static <T> CursorPage<T> of(List<T> fetched, int size, Function<T, String> cursorOf) {
        boolean hasMore = fetched.size() > size;
        List<T> page = hasMore ? fetched.subList(0, size) : fetched;
        return new CursorPage<>(page, hasMore ? cursorOf.apply(page.get(page.size() - 1)) : null);
    }

    /**
     * Normaliza o {@code size} pedido pelo cliente — todo parâmetro de
     * request é input hostil até validado: satura em {@link #MAX_PAGE_SIZE}
     * por cima (semântica já decidida no contrato M2) e em {@code 1} por
     * baixo ({@code 0}/negativo estourariam como 500 dentro de {@code
     * ExecutionQuery}/{@link #of}, nunca o mecanismo certo de validação).
     */
    public static int clampSize(@Nullable Integer requested) {
        return requested == null ? DEFAULT_PAGE_SIZE : Math.clamp(requested, 1, MAX_PAGE_SIZE);
    }
}
