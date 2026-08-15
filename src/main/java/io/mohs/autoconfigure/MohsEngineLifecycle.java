package io.mohs.autoconfigure;

import java.time.Duration;
import java.util.Objects;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

import io.mohs.core.EngineState;
import io.mohs.core.MohsLifecycle;
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
    private final Duration leaseTtl;

    MohsEngineLifecycle(MohsLifecycle engine, boolean autoStartup, Duration shutdownGracePeriod, JobStore jobStore, Duration leaseTtl) {
        this.engine = Objects.requireNonNull(engine, "engine");
        this.autoStartup = autoStartup;
        this.shutdownGracePeriod = Objects.requireNonNull(shutdownGracePeriod, "shutdownGracePeriod");
        this.jobStore = Objects.requireNonNull(jobStore, "jobStore");
        this.leaseTtl = Objects.requireNonNull(leaseTtl, "leaseTtl");
    }

    @Override
    public void start() {
        warnIfLeaseShorterThanDeclaredTimeouts();
        engine.start();
    }

    /**
     * Detecção de falha por timeout é sempre uma aposta (ADR-0012): sem
     * watchdog que renove a lease de execuções {@code RUNNING} (limitação
     * declarada do {@code Engine}), um handler saudável que rode além de
     * {@code mohs.engine.lease-ttl} é reclamado como {@code FAILED} pelo
     * reaper e a vaga de concorrência do job é devolvida — pra job com
     * exclusão ({@code preventOverlap}), isso é dupla execução em paralelo
     * com o handler zumbi. O operador precisa saber o preço do default no
     * boot, não no postmortem — daí o WARN por job cujo {@code timeout}
     * declarado alcança a lease. Diagnóstico nunca derruba o boot: falha
     * na leitura vira WARN com a causa completa, o engine sobe igual.
     */
    private void warnIfLeaseShorterThanDeclaredTimeouts() {
        try (Stream<StoredJob> jobs = jobStore.findAll()) {
            jobs.map(StoredJob::definition)
                    .filter(definition -> definition.timeout() != null && definition.timeout().compareTo(leaseTtl) >= 0)
                    .forEach(definition -> log.warn(
                            "job '{}' declares timeout {} >= mohs.engine.lease-ttl {} — without a lease-renewing watchdog, "
                                    + "a healthy run longer than the lease is reaped as FAILED and its concurrency slot is released; "
                                    + "raise mohs.engine.lease-ttl above the slowest expected attempt",
                            definition.key().value(), definition.timeout(), leaseTtl));
        } catch (RuntimeException e) {
            log.warn("could not check job timeouts against mohs.engine.lease-ttl on startup", e);
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
