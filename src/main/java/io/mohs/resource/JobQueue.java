package io.mohs.resource;

import java.util.Objects;

/**
 * Cap de concorrência cluster-wide sobre um recurso compartilhado (SMTP,
 * API parceira) — papel distinto de {@link MohsRunner}, que protege só
 * este nó; escalar nós não pode escalar a pressão sobre o recurso. Bean
 * define a estrutura, property ajusta o número
 * ({@code mohs.queues.<nome>.max-concurrent}).
 */
public record JobQueue(String name, int maxConcurrent) {

    public JobQueue {
        Objects.requireNonNull(name, "name");
        if (name.isBlank()) {
            throw new IllegalArgumentException("name must not be blank");
        }
        if (maxConcurrent < 1) {
            throw new IllegalArgumentException("maxConcurrent must be at least 1");
        }
    }

    public static Builder named(String name) {
        return new Builder(name);
    }

    public static final class Builder {
        private final String name;
        // Default conservador de propósito: "backpressure e limites em toda
        // borda" (CLAUDE.md) — uma queue esquecida sem maxConcurrent(...)
        // deve travar cedo, não vazar concorrência ilimitada.
        private int maxConcurrent = 1;

        private Builder(String name) {
            this.name = name;
        }

        public Builder maxConcurrent(int max) {
            this.maxConcurrent = max;
            return this;
        }

        public JobQueue build() {
            return new JobQueue(name, maxConcurrent);
        }
    }
}
