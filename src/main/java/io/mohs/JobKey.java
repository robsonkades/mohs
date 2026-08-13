package io.mohs;

import java.util.Objects;

/**
 * Stable identity of a {@link JobDefinition}, used as its persistence key.
 * Unlike {@link JobDefinition#name()}, a {@code JobKey}'s value never
 * changes for a given job — renaming a job's label does not change its key.
 */
public record JobKey(String value) {

    public JobKey {
        Objects.requireNonNull(value, "value");
        if (value.isBlank()) {
            throw new IllegalArgumentException("JobKey value must not be blank");
        }
    }

    /**
     * Static factory over a public constructor (Effective Java, Item 1):
     * reads as a conversion at call sites such as {@code JobKey.of(id)}.
     */
    public static JobKey of(String value) {
        return new JobKey(value);
    }
}
