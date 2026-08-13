package io.mohs.core.resource;

import java.time.Duration;
import java.util.Objects;

/**
 * Vazão máxima permitida sobre um recurso compartilhado numa janela de
 * tempo — mesma família de {@link JobQueue} (cap cluster-wide), mas
 * limitando taxa em vez de concorrência simultânea; os dois papéis
 * coexistem sobre o mesmo recurso quando fizer sentido (ex.: SMTP com
 * {@code maxConcurrent} e {@code max}/{@code window} ao mesmo tempo). Bean
 * define a estrutura, property ajusta os números
 * ({@code mohs.rate-limits.<nome>.max}/{@code .window}). Spec, nunca
 * limitador de fato — quem aplica é o motor (M3).
 */
public record RateLimit(String name, int max, Duration window) {

    public RateLimit {
        Objects.requireNonNull(name, "name");
        if (name.isBlank()) {
            throw new IllegalArgumentException("name must not be blank");
        }
        if (max < 1) {
            throw new IllegalArgumentException("max must be at least 1");
        }
        Objects.requireNonNull(window, "window");
        if (!window.isPositive()) {
            throw new IllegalArgumentException("window must be positive");
        }
    }
}
