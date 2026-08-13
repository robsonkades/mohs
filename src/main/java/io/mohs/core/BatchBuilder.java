package io.mohs.core;

import io.mohs.core.job.JobRef;

/** Acumulador de um lote flat — cada {@link #add} agenda um job dentro do mesmo lote. */
public interface BatchBuilder {
    <T> void add(JobRef<T> ref, T payload);
}
