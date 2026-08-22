package io.mohs.autoconfigure;

import java.time.Duration;
import java.util.Objects;
import java.util.stream.Stream;

import org.jspecify.annotations.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

import io.mohs.core.EngineState;
import io.mohs.core.MohsLifecycle;
import io.mohs.core.definition.JobDefinition;
import io.mohs.engine.JobStore;
import io.mohs.engine.StoredJob;

/**
 * Adapta {@link MohsLifecycle} (domínio, {@code start()}/{@code stop(Duration)})
 * para {@link SmartLifecycle} (Spring, {@code start()}/{@code stop()}) — são
 * interfaces de forma parecida mas assinatura incompatível, não a mesma
 * coisa com nome diferente. Fase tardia ({@link SmartLifecycle#DEFAULT_PHASE}): sobe
 * por último (depois de qualquer bean de registro de job que vier a existir
 * — ADR-0006 exige nenhum claim antes de todas as definições anotadas
 * estarem registradas) e desce primeiro.
 */
final class MohsEngineLifecycle implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(MohsEngineLifecycle.class);

    private final MohsLifecycle engine;
    private final boolean autoStartup;
    private final Duration shutdownGracePeriod;
    private final JobStore jobStore;
    private final @Nullable Duration watchdogTimeout;

    MohsEngineLifecycle(MohsLifecycle engine, boolean autoStartup, Duration shutdownGracePeriod, JobStore jobStore,
            @Nullable Duration watchdogTimeout) {
        this.engine = Objects.requireNonNull(engine, "engine");
        this.autoStartup = autoStartup;
        this.shutdownGracePeriod = Objects.requireNonNull(shutdownGracePeriod, "shutdownGracePeriod");
        this.jobStore = Objects.requireNonNull(jobStore, "jobStore");
        this.watchdogTimeout = watchdogTimeout;
    }

    @Override
    public void start() {
        warnAboutDeclaredPolicyGaps();
        engine.start();
    }

    /**
     * Avisos de boot sobre lacunas entre o que a definição declara e o que
     * o motor entrega — o operador precisa saber o preço no boot, não no
     * postmortem. Duas checagens, uma passada só pelo store:
     * (1) com a liveness por nó (ADR-0051), handler lento saudável não é
     * mais reclamado — o risco restante é o Watchdog Bound
     * ({@code mohs.engine.watchdog-timeout}) menor que o {@code timeout}
     * declarado do job: o node liberaria a posse antes do prazo que o
     * próprio job se deu, falhando uma execução ainda saudável;
     * (2) {@code retryPolicy} (bean customizado) ainda não é
     * honrada (ADR-0033) — só {@code retries} vale, com o backoff default.
     * Diagnóstico nunca derruba o boot: falha na leitura vira WARN com a
     * causa completa, o engine sobe igual.
     */
    private void warnAboutDeclaredPolicyGaps() {
        try (Stream<StoredJob> jobs = jobStore.findAll()) {
            jobs.map(StoredJob::definition).forEach(definition -> {
                warnIfTimeoutOutlivesWatchdogBound(definition);
                warnIfRetryPolicyNotHonored(definition);
            });
        } catch (RuntimeException e) {
            log.warn("could not check declared job policies on startup", e);
        }
    }

    private void warnIfTimeoutOutlivesWatchdogBound(JobDefinition definition) {
        if (watchdogTimeout != null && definition.timeout() != null && definition.timeout().compareTo(watchdogTimeout) >= 0) {
            log.warn(
                    "job '{}' declares timeout {} >= mohs.engine.watchdog-timeout {} — the watchdog releases ownership "
                            + "before the job's own deadline, failing a still-healthy run (retry budget applies); "
                            + "raise mohs.engine.watchdog-timeout above the slowest declared job timeout",
                    definition.key().value(), definition.timeout(), watchdogTimeout);
        }
    }

    private static void warnIfRetryPolicyNotHonored(JobDefinition definition) {
        if (definition.retryPolicy() != null) {
            log.warn(
                    "job '{}' declares retryPolicy '{}' which is not honored yet — retries is, with the default "
                            + "exponential full-jitter backoff (ADR-0033)",
                    definition.key().value(), definition.retryPolicy());
        }
    }

    @Override
    public void stop() {
        engine.stop(shutdownGracePeriod);
    }

    @Override
    public boolean isRunning() {
        EngineState state = engine.state();
        return state != EngineState.CREATED && state != EngineState.STOPPED;
    }

    @Override
    public boolean isAutoStartup() {
        return autoStartup;
    }

    /**
     * Explícito mesmo sendo o default da interface: a fase é garantia
     * arquitetural documentada (ADR-0006 — sobe por último, desce
     * primeiro), não coincidência de default.
     */
    @Override
    public int getPhase() {
        return SmartLifecycle.DEFAULT_PHASE;
    }
}
