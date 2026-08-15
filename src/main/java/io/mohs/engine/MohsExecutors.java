package io.mohs.engine;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ThreadPoolExecutor;

import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

/**
 * Fábrica central de executor/scheduler do Mohs — nenhuma classe do motor
 * cria {@code Executor}/{@code ScheduledExecutorService} na mão; todas
 * recebem o que esta fábrica constrói, injetado no construtor (quem chama
 * também é dono do ciclo de vida do que recebe de volta — esta fábrica
 * nunca desliga o que cria, ver Javadoc de cada método). Ponto único onde
 * a disciplina de concorrência do CLAUDE.md vira código, não convenção
 * repetida em cada classe nova: threads sempre nomeadas
 * ({@code mohs-<recurso>-N}), I/O-bound sempre virtual thread com limite
 * real via {@code Semaphore} (nunca por tamanho de pool), CPU-bound sempre
 * pool limitado explícito, nunca fila sem teto "por omissão do Spring"
 * ({@link ThreadPoolTaskExecutor} sem {@code setQueueCapacity} explícito
 * herdaria fila efetivamente ilimitada — exatamente o que o CLAUDE.md
 * proíbe).
 *
 * <p>"O motor usa a infraestrutura Spring livremente"
 * ({@code MOHS-DOCUMENTO-MESTRE.md} §4) — as três classes usadas aqui já
 * implementam boa parte do que o motor precisaria reescrever na mão:
 * {@code ExecutorConfigurationSupport} (classe-mãe de {@link
 * ThreadPoolTaskExecutor}/{@link ThreadPoolTaskScheduler}) implementa
 * {@code SmartLifecycle} diretamente — o mesmo gancho que a ADR-0007 já
 * escolheu pro shutdown gracioso do motor.
 *
 * <p>Métodos estáticos hoje porque {@code io.mohs.autoconfigure} ainda não
 * existe pra hospedar {@code @Bean}; quando existir, cada método aqui vira
 * um wrapper de uma linha (o corpo não muda), não uma reescrita.
 */
public final class MohsExecutors {

    /** Mesmo default de {@code mohs.lifecycle.shutdown.grace-period} (ADR-0007) — não um número arbitrário escolhido à parte. */
    private static final int AWAIT_TERMINATION_SECONDS = 30;

    private MohsExecutors() {
    }

    /**
     * I/O-bound: uma virtual thread por tarefa, teto de concorrência real
     * via {@code Semaphore} interno do Spring — nunca por tamanho de pool
     * (CLAUDE.md, "Limite de concorrência com Semaphore, nunca via tamanho
     * de pool"). Acima do limite, {@link java.util.concurrent.RejectedExecutionException}
     * explícita — backpressure, não fila escondida nem espera infinita.
     *
     * <p>Ciclo de vida por conta de quem chama: devolve o tipo concreto
     * (não a interface {@code AsyncTaskExecutor}, ao contrário do que este
     * método fazia antes) porque {@link SimpleAsyncTaskExecutor} é
     * {@code AutoCloseable} ({@code close()}), não tem {@code shutdown()} —
     * quem recebeu o executor de volta precisa do tipo concreto pra poder
     * fechá-lo, mesma razão de {@link #cpuBoundExecutor}/{@link #scheduler}
     * devolverem o tipo concreto deles.
     */
    public static SimpleAsyncTaskExecutor ioBoundExecutor(String namePrefix, int concurrencyLimit) {
        requireNotBlank(namePrefix, "namePrefix");
        if (concurrencyLimit <= 0) {
            throw new IllegalArgumentException("concurrencyLimit must be positive");
        }

        SimpleAsyncTaskExecutor executor = new SimpleAsyncTaskExecutor(threadNamePrefix(namePrefix));
        executor.setVirtualThreads(true);
        executor.setConcurrencyLimit(concurrencyLimit);
        executor.setRejectTasksWhenLimitReached(true);
        executor.setTaskTerminationTimeout(Duration.ofSeconds(AWAIT_TERMINATION_SECONDS).toMillis());
        return executor;
    }

    /**
     * CPU-bound: pool de platform threads limitado — mesmos quatro campos e
     * mesma validação de {@link io.mohs.core.resource.MohsRunner#cpu}
     * (coincidência não é acaso: o spec já foi desenhado nesse formato).
     *
     * <p>Chama {@code initialize()} antes de devolver — sem
     * {@code ApplicationContext} gerenciando o bean, ninguém mais chamaria
     * (a inicialização de {@code ExecutorConfigurationSupport} normalmente
     * vem de {@code InitializingBean#afterPropertiesSet}). Ciclo de vida
     * por conta de quem chama a partir daqui: esta fábrica não desliga o
     * que cria.
     */
    public static ThreadPoolTaskExecutor cpuBoundExecutor(String namePrefix, int coreSize, int maxSize, int queueCapacity, Duration keepAlive) {
        requireNotBlank(namePrefix, "namePrefix");
        if (coreSize < 1) {
            throw new IllegalArgumentException("coreSize must be at least 1");
        }
        if (maxSize < coreSize) {
            throw new IllegalArgumentException("maxSize must be >= coreSize");
        }
        if (queueCapacity < 0) {
            throw new IllegalArgumentException("queueCapacity must not be negative");
        }
        Objects.requireNonNull(keepAlive, "keepAlive");
        if (keepAlive.isNegative()) {
            throw new IllegalArgumentException("keepAlive must not be negative");
        }

        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setThreadNamePrefix(threadNamePrefix(namePrefix));
        executor.setCorePoolSize(coreSize);
        executor.setMaxPoolSize(maxSize);
        executor.setQueueCapacity(queueCapacity);
        executor.setKeepAliveSeconds((int) keepAlive.toSeconds());
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.AbortPolicy());
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(AWAIT_TERMINATION_SECONDS);
        executor.initialize();
        return executor;
    }

    /**
     * Scheduler dedicado — hoje só o tick do poll loop ({@link Engine}),
     * genérico o bastante pra qualquer ciclo futuro. Mesma observação de
     * {@link #cpuBoundExecutor} sobre {@code initialize()}/ciclo de vida.
     */
    public static ThreadPoolTaskScheduler scheduler(String namePrefix, int poolSize) {
        requireNotBlank(namePrefix, "namePrefix");
        if (poolSize < 1) {
            throw new IllegalArgumentException("poolSize must be at least 1");
        }

        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setThreadNamePrefix(threadNamePrefix(namePrefix));
        scheduler.setPoolSize(poolSize);
        scheduler.setRejectedExecutionHandler(new ThreadPoolExecutor.AbortPolicy());
        scheduler.setWaitForTasksToCompleteOnShutdown(true);
        scheduler.setAwaitTerminationSeconds(AWAIT_TERMINATION_SECONDS);
        scheduler.initialize();
        return scheduler;
    }

    private static String threadNamePrefix(String namePrefix) {
        return namePrefix.endsWith("-") ? namePrefix : namePrefix + "-";
    }

    /** Mesma checagem de {@code io.mohs.core.resource.Fields#requireNotBlank}, não reaproveitada de lá: pacote diferente, não vale expor como API pública por isto (mesmo raciocínio já registrado naquela classe). */
    private static void requireNotBlank(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
    }
}
