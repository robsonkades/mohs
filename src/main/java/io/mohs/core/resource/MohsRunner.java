package io.mohs.core.resource;

import java.time.Duration;
import java.util.Objects;

/**
 * Capacidade de execução node-local, referenciada por nome estilo
 * {@code @Async("...")} — mas o bean é este spec, nunca um
 * {@code java.util.concurrent.Executor}: o Mohs cria e é dono das threads,
 * requisito para cancelamento cooperativo, timeout por interrupt, métricas
 * por runner e a disciplina io→virtual/cpu→platform (Effective Java Item 64:
 * referencie pela interface/spec, não pela implementação concreta).
 *
 * <p>Record único e flat, não selado por modo: {@link #maxConcurrent()}
 * só é válido para {@link RunnerMode#IO}; {@link #coreSize()},
 * {@link #maxSize()}, {@link #queueCapacity()} e {@link #keepAlive()} só
 * para {@link RunnerMode#CPU} — o campo do modo errado fica zerado e
 * ignorado. Ergonomia (impedir {@code .coreSize()} num runner IO) é
 * responsabilidade dos dois builders separados ({@link IoBuilder}/
 * {@link CpuBuilder}), não do tipo armazenado (ver
 * {@code docs/adr/0014-cpu-runner-pool-properties.md} para a alternativa
 * selada considerada e rejeitada).
 *
 * <p>As quatro properties de {@code CPU} espelham
 * {@code org.springframework.boot.task.TaskExecutionProperties.Pool} do
 * Spring Boot (core-size/max-size/queue-capacity/keep-alive) — mesma ideia
 * de {@code spring.task.execution.pool.*} — mas com defaults diferentes,
 * deliberadamente: o Spring default para pool/fila efetivamente ilimitados
 * porque não sabe se o trabalho é CPU ou I/O; aqui sabemos que é
 * CPU-bound, e "backpressure em toda borda... nunca espera infinita"
 * (CLAUDE.md) já é regra do projeto. Ver a ADR-0014 para o raciocínio
 * completo dos defaults.
 */
public record MohsRunner(String name, RunnerMode mode, int maxConcurrent, int coreSize, int maxSize, int queueCapacity, Duration keepAlive) {

    public MohsRunner {
        Objects.requireNonNull(name, "name");
        if (name.isBlank()) {
            throw new IllegalArgumentException("name must not be blank");
        }
        Objects.requireNonNull(mode, "mode");
        Objects.requireNonNull(keepAlive, "keepAlive");
        if (mode == RunnerMode.IO) {
            if (maxConcurrent < 1) {
                throw new IllegalArgumentException("maxConcurrent must be at least 1");
            }
        } else {
            if (coreSize < 1) {
                throw new IllegalArgumentException("coreSize must be at least 1");
            }
            if (maxSize < coreSize) {
                throw new IllegalArgumentException("maxSize must be >= coreSize");
            }
            if (queueCapacity < 0) {
                throw new IllegalArgumentException("queueCapacity must not be negative");
            }
            if (keepAlive.isNegative()) {
                throw new IllegalArgumentException("keepAlive must not be negative");
            }
        }
    }

    /** Runner de I/O; default {@code maxConcurrent = 64} (§3 do documento mestre). */
    public static IoBuilder io(String name) {
        return new IoBuilder(name);
    }

    /** Runner de CPU; defaults dimensionados para núcleos disponíveis (§3 do documento mestre). */
    public static CpuBuilder cpu(String name) {
        return new CpuBuilder(name);
    }

    /** Builder do runner {@link RunnerMode#IO} — só o permit count do {@code Semaphore} que limita concorrência. */
    public static final class IoBuilder {
        private final String name;
        private int maxConcurrent = 64;

        private IoBuilder(String name) {
            this.name = name;
        }

        public IoBuilder maxConcurrent(int max) {
            this.maxConcurrent = max;
            return this;
        }

        public MohsRunner build() {
            return new MohsRunner(name, RunnerMode.IO, maxConcurrent, 0, 0, 0, Duration.ZERO);
        }
    }

    /**
     * Builder do runner {@link RunnerMode#CPU} — as quatro properties de
     * pool estilo Spring. {@code maxSize} default igual a {@code coreSize}
     * (pool fixo, sem elasticidade: mais threads que núcleos não ajuda
     * trabalho CPU-bound); {@code queueCapacity} default 0 (direct
     * handoff — cresce até {@code maxSize}, depois rejeita na hora, sem
     * fila escondida); {@code keepAlive} só tem efeito quando
     * {@code maxSize > coreSize} é configurado explicitamente (semântica
     * padrão de {@code ThreadPoolExecutor}: keep-alive não se aplica a
     * core threads por default).
     */
    public static final class CpuBuilder {
        private final String name;
        private int coreSize = Runtime.getRuntime().availableProcessors();
        private int maxSize = Runtime.getRuntime().availableProcessors();
        private int queueCapacity = 0;
        private Duration keepAlive = Duration.ofSeconds(60);

        private CpuBuilder(String name) {
            this.name = name;
        }

        public CpuBuilder coreSize(int coreSize) {
            this.coreSize = coreSize;
            return this;
        }

        public CpuBuilder maxSize(int maxSize) {
            this.maxSize = maxSize;
            return this;
        }

        public CpuBuilder queueCapacity(int queueCapacity) {
            this.queueCapacity = queueCapacity;
            return this;
        }

        public CpuBuilder keepAlive(Duration keepAlive) {
            this.keepAlive = keepAlive;
            return this;
        }

        public MohsRunner build() {
            return new MohsRunner(name, RunnerMode.CPU, 0, coreSize, maxSize, queueCapacity, keepAlive);
        }
    }
}
