package io.mohs.core.execution;

import java.util.Objects;

/**
 * Identidade de uma única {@link Execution}. Opaco por design — o motor
 * decide o formato concreto (ex.: ULID) quando começar a gerá-los em
 * {@code io.mohs.store.jdbc}; este contrato só exige um valor não vazio.
 */
public record ExecutionId(String value) {

    public ExecutionId {
        Objects.requireNonNull(value, "value");
        if (value.isBlank()) {
            throw new IllegalArgumentException("ExecutionId value must not be blank");
        }
    }

    public static ExecutionId of(String value) {
        return new ExecutionId(value);
    }
}
