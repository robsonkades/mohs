package io.mohs.core;

import java.time.Duration;

/**
 * Ciclo de vida do engine neste nó — não confundir com pause de job
 * (cluster-wide, por job, via {@link Mohs}/REST). Ver
 * {@code docs/adr/0007-engine-lifecycle.md}.
 */
public interface MohsLifecycle {

    EngineState state();

    void start();

    void pause();

    void resume();

    /** Para de aceitar claims; espera o in-flight até {@code grace}. Drain ≠ cancel. */
    void drain(Duration grace);

    /** {@link #drain} seguido do desligamento dos runners. */
    void stop(Duration grace);
}
