package io.mohs.core.job;

import io.mohs.core.Mohs;

import java.util.Objects;

/**
 * Referência tipada a uma definição de job, amarrando seu {@link JobKey} ao
 * tipo do payload em tempo de compilação. Passar um {@code JobRef<WelcomeEmail>}
 * para {@link Mohs#schedule(JobRef, Object)} torna um payload incompatível
 * com a definição um erro de compilação, não uma surpresa em runtime — o
 * ponto do "tipado > stringly" (ver
 * {@code docs/adr/0002-definition-vs-invocation.md}).
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
