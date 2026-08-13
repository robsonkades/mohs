package io.mohs;

import java.util.Objects;

/**
 * Identity of a single {@link Execution}. Opaque by design — the engine
 * decides the concrete format (e.g. ULID) when it starts minting these in
 * {@code io.mohs.jdbc}; this contract only requires a non-blank value.
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
