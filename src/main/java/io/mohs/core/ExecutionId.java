package io.mohs.core;

import java.util.Objects;

import io.mohs.core.execution.Execution;

/**
 * Identidade de uma única {@link Execution}. Opaco por design — o motor
 * decide o formato concreto (ex.: ULID) quando começar a gerá-los em
 * {@code io.mohs.jdbc}; este contrato só exige um valor não vazio.
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
