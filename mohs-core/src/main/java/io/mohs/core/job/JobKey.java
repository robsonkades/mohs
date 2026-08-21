package io.mohs.core.job;

import java.util.Objects;

import io.mohs.core.definition.JobDefinition;

/**
 * Identidade estável de um {@link JobDefinition}, usada como sua chave de
 * persistência. Ao contrário de {@link JobDefinition#name()}, o valor de
 * um {@code JobKey} nunca muda para um dado job — renomear o rótulo de um
 * job não muda sua chave.
 */
public record JobKey(String value) {

    public JobKey {
        Objects.requireNonNull(value, "value");
        if (value.isBlank()) {
            throw new IllegalArgumentException("JobKey value must not be blank");
        }
    }

    /**
     * Fábrica estática em vez de construtor público (Effective Java, Item 1):
     * lê como uma conversão no ponto de chamada, ex. {@code JobKey.of(id)}.
     */
    public static JobKey of(String value) {
        return new JobKey(value);
    }
}
