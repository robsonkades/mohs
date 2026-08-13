package io.mohs;

/**
 * Estado do ciclo de vida do engine neste nó — {@code CREATED → RUNNING ⇄
 * PAUSED → DRAINING → STOPPED}. Node-local por natureza; não confundir com
 * pause de job, que é cluster-wide e por job (ver
 * {@code docs/adr/0007-engine-lifecycle.md}).
 */
public enum EngineState {
    CREATED,
    RUNNING,
    PAUSED,
    DRAINING,
    STOPPED
}
