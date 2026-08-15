package io.mohs.engine;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.jspecify.annotations.Nullable;

import io.mohs.core.job.JobKey;

/**
 * Registro em memória de {@code JobKey → JobHandler} — a costura
 * manual que {@link Dispatcher} consulta pra saber o que chamar.
 * {@code io.mohs.autoconfigure} (fora de escopo ainda) povoa isto
 * escaneando {@code @MohsJob} no boot; até lá, quem já tem a referência em
 * mãos (testes, código que registra na mão) usa {@link #register} direto —
 * mesmo espírito de {@code io.mohs.test.InMemoryJobStore} como costura
 * manual de {@code JobStore}, sem JDBC.
 *
 * <p>{@code payloadType} é opcional: só o scanner de {@code @MohsJob}
 * sabe o tipo real do parâmetro (reflection no método anotado); registro
 * manual (testes, {@link #register(JobKey, JobHandler)}) não tem essa
 * informação e não precisa dela — {@link #payloadType} existe só pra
 * REST (M3) converter JSON pro tipo esperado antes de agendar.
 */
public final class HandlerRegistry {

    private final Map<JobKey, Registration> invocations = new ConcurrentHashMap<>();

    public void register(JobKey key, JobHandler invocation) {
        register(key, invocation, null);
    }

    public void register(JobKey key, JobHandler invocation, @Nullable Class<?> payloadType) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(invocation, "invocation");
        invocations.put(key, new Registration(invocation, payloadType));
    }

    public Optional<JobHandler> find(JobKey key) {
        Objects.requireNonNull(key, "key");
        return Optional.ofNullable(invocations.get(key)).map(Registration::handler);
    }

    public Optional<Class<?>> payloadType(JobKey key) {
        Objects.requireNonNull(key, "key");
        return Optional.ofNullable(invocations.get(key)).map(Registration::payloadType);
    }

    private record Registration(JobHandler handler, @Nullable Class<?> payloadType) {
    }
}
