package io.mohs;

import java.util.Objects;

/**
 * Typed reference to a job definition, binding its {@link JobKey} to the
 * payload type at compile time. Passing a {@code JobRef<WelcomeEmail>} to
 * {@link Mohs#schedule(JobRef, Object)} makes a payload/definition mismatch
 * a compile error instead of a runtime surprise — the point of "typed over
 * stringly" (see {@code docs/adr/0002-definition-vs-invocation.md}).
 */
public record JobRef<T>(JobKey key, Class<T> payloadType) {

    public JobRef {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(payloadType, "payloadType");
    }

    public static <T> JobRef<T> of(String id, Class<T> payloadType) {
        return new JobRef<>(JobKey.of(id), payloadType);
    }
}
