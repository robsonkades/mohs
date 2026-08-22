package io.mohs.core.resource;

import java.time.Duration;
import java.util.Objects;

/**
 * Vazão máxima permitida sobre um recurso compartilhado numa janela de
 * tempo — cap cluster-wide, mas limitando taxa em vez de concorrência
 * simultânea. Bean define a estrutura, property ajusta os números
 * ({@code mohs.rate-limits.<nome>.max}/{@code .window}). Spec, nunca
 * limitador de fato — quem aplica é o motor (ADR-0042).
 */
public record RateLimit(String name, int max, Duration window) {

    public RateLimit {
        Fields.requireNotBlank(name, "name");
        requireRefillable(max, window);
    }

    /**
     * A regra de vazão isolada do nome, para quem valida antes de ter um
     * ({@code PATCH /rate-limits/{name}} recebe o nome pelo path, não pelo
     * corpo). Fonte única: o wire e o record aplicam ISTO, nunca duas
     * cópias que divergem na terceira edição.
     *
     * <p>O teto de {@code max} pela janela em nanos é o que impede
     * {@code window.dividedBy(max)} de truncar para zero: o intervalo de
     * refill do balde (ADR-0042) precisa ser representável, e uma divisão
     * por {@code Duration.ZERO} lá dentro derrubaria a rodada de claim
     * INTEIRA — inclusive os jobs sem limite nenhum.
     */
    public static void requireRefillable(int max, Duration window) {
        if (max < 1) {
            throw new IllegalArgumentException("max must be at least 1");
        }
        Objects.requireNonNull(window, "window");
        if (!window.isPositive()) {
            throw new IllegalArgumentException("window must be positive");
        }
        if (window.compareTo(Duration.ofNanos(max)) < 0) {
            throw new IllegalArgumentException("window " + window + " is too short for max " + max
                    + " — one token is issued every window/max, which needs at least " + max + "ns of window");
        }
    }
}
