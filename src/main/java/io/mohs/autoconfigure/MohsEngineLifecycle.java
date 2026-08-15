package io.mohs.autoconfigure;

import java.time.Duration;
import java.util.Objects;

import org.springframework.context.SmartLifecycle;

import io.mohs.core.EngineState;
import io.mohs.core.MohsLifecycle;

/**
 * Adapta {@link MohsLifecycle} (domínio, {@code start()}/{@code stop(Duration)})
 * para {@link SmartLifecycle} (Spring, {@code start()}/{@code stop()}) — são
 * interfaces de forma parecida mas assinatura incompatível, não a mesma
 * coisa com nome diferente. Fase tardia ({@link Integer#MAX_VALUE}): sobe
 * por último (depois de qualquer bean de registro de job que vier a existir
 * — ADR-0006 exige nenhum claim antes de todas as definições anotadas
 * estarem registradas) e desce primeiro.
 */
final class MohsEngineLifecycle implements SmartLifecycle {

    private final MohsLifecycle engine;
    private final boolean autoStartup;
    private final Duration shutdownGracePeriod;

    MohsEngineLifecycle(MohsLifecycle engine, boolean autoStartup, Duration shutdownGracePeriod) {
        this.engine = Objects.requireNonNull(engine, "engine");
        this.autoStartup = autoStartup;
        this.shutdownGracePeriod = Objects.requireNonNull(shutdownGracePeriod, "shutdownGracePeriod");
    }

    @Override
    public void start() {
        engine.start();
    }

    @Override
    public void stop() {
        engine.stop(shutdownGracePeriod);
    }

    @Override
    public void stop(Runnable callback) {
        stop();
        callback.run();
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

    @Override
    public int getPhase() {
        return Integer.MAX_VALUE;
    }
}
