package io.mohs.schedule;

/**
 * Política aplicada quando um disparo agendado é perdido (nó fora do ar,
 * motor pausado, salto de relógio). {@link #IGNORE} é o default: um disparo
 * perdido é simplesmente ignorado, o job retoma na próxima ocorrência
 * regular.
 */
public enum Misfire {

    /** Ignora disparos perdidos; retoma na próxima ocorrência regular. Default. */
    IGNORE,

    /** Dispara uma vez imediatamente para a ocorrência perdida mais recente. */
    FIRE_NOW,

    /** Reproduz cada ocorrência perdida, com teto, drenada, nunca descartada. */
    FIRE_ALL_MISSED
}
