package io.mohs;

import java.util.Objects;

/**
 * Capacidade de execução node-local, referenciada por nome estilo
 * {@code @Async("...")} — mas o bean é este spec, nunca um
 * {@code java.util.concurrent.Executor}: o Mohs cria e é dono das threads,
 * requisito para cancelamento cooperativo, timeout por interrupt, métricas
 * por runner e a disciplina io→virtual/cpu→platform (Effective Java Item 64:
 * referencie pela interface/spec, não pela implementação concreta).
 */
public record MohsRunner(String name, RunnerMode mode, int maxConcurrent) {

    public MohsRunner {
        Objects.requireNonNull(name, "name");
        if (name.isBlank()) {
            throw new IllegalArgumentException("name must not be blank");
        }
        Objects.requireNonNull(mode, "mode");
        if (maxConcurrent < 1) {
            throw new IllegalArgumentException("maxConcurrent must be at least 1");
        }
    }

    /** Runner de I/O; default {@code maxConcurrent = 64} (§3 do documento mestre). */
    public static Builder io(String name) {
        return new Builder(name, RunnerMode.IO, 64);
    }

    /** Runner de CPU; default {@code maxConcurrent} = núcleos disponíveis (§3 do documento mestre). */
    public static Builder cpu(String name) {
        return new Builder(name, RunnerMode.CPU, Runtime.getRuntime().availableProcessors());
    }

    public static final class Builder {
        private final String name;
        private final RunnerMode mode;
        private int maxConcurrent;

        private Builder(String name, RunnerMode mode, int defaultMaxConcurrent) {
            this.name = name;
            this.mode = mode;
            this.maxConcurrent = defaultMaxConcurrent;
        }

        public Builder maxConcurrent(int max) {
            this.maxConcurrent = max;
            return this;
        }

        public MohsRunner build() {
            return new MohsRunner(name, mode, maxConcurrent);
        }
    }
}
