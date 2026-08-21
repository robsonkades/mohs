package io.mohs.rest.batch;

/** Estado agregado de um lote — {@code COMPLETED} quando não sobra nenhuma execução pendente. */
public enum BatchState {
    RUNNING,
    COMPLETED
}
