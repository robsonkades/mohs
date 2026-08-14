package io.mohs.engine;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import io.mohs.core.job.JobKey;

/**
 * Registro em memória de {@code JobKey → HandlerInvocation} — a costura
 * manual que {@link Dispatcher} consulta pra saber o que chamar.
 * {@code io.mohs.autoconfigure} (fora de escopo ainda) povoa isto
 * escaneando {@code @MohsJob} no boot; até lá, quem já tem a referência em
 * mãos (testes, código que registra na mão) usa {@link #register} direto —
 * mesmo espírito de {@code io.mohs.test.InMemoryJobStore} como costura
 * manual de {@code JobStore}, sem JDBC.
 */
public final class HandlerRegistry {

    private final Map<JobKey, HandlerInvocation> invocations = new ConcurrentHashMap<>();

    public void register(JobKey key, HandlerInvocation invocation) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(invocation, "invocation");
        invocations.put(key, invocation);
    }

    public Optional<HandlerInvocation> find(JobKey key) {
        Objects.requireNonNull(key, "key");
        return Optional.ofNullable(invocations.get(key));
    }
}
