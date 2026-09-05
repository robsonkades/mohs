/*
 * Copyright 2026 The Mohs Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.mohs.engine;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.jspecify.annotations.Nullable;

import io.mohs.core.job.JobKey;

/**
 * The in-memory {@code JobKey} to {@code JobHandler} registry — the manual seam {@link Dispatcher}
 * consults to know what to call.
 *
 * <p>{@code io.mohs.autoconfigure} populates it by scanning for {@code @MohsJob} at boot; anyone who
 * already has the reference in hand (tests, code registering by hand) uses {@link #register}
 * directly — the same spirit as {@code io.mohs.test.InMemoryJobStore} being a manual seam for
 * {@code JobStore}, with no JDBC.
 *
 * <p>{@code payloadType} is optional: only the {@code @MohsJob} scanner knows the parameter's real
 * type (through reflection on the annotated method); a manual registration
 * ({@link #register(JobKey, JobHandler)}) does not have that information and does not need it —
 * {@link #payloadType} exists only so REST can convert JSON to the expected type before scheduling.
 */
public final class HandlerRegistry {

    /**
     * Creates an empty local handler registry.
     */
    public HandlerRegistry() {
    }

    private final Map<JobKey, Registration> invocations = new ConcurrentHashMap<>();

    /**
     * Registers a local handler and its payload contract.
     *
     * @param key the stable identity of the job
     * @param invocation the callable job handler
     */
    public void register(JobKey key, JobHandler invocation) {
        register(key, invocation, null);
    }

    /**
     * Registers a local handler and its payload contract.
     *
     * @param key the stable identity of the job
     * @param invocation the callable job handler
     * @param payloadType the runtime payload class, or {@code null} when the handler declares none
     */
    public void register(JobKey key, JobHandler invocation, @Nullable Class<?> payloadType) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(invocation, "invocation");
        invocations.put(key, new Registration(invocation, payloadType));
    }

    /**
     * Looks up the locally registered handler.
     *
     * @param key the stable identity of the job
     * @return the handler, or empty when the key is not registered
     */
    public Optional<JobHandler> find(JobKey key) {
        Objects.requireNonNull(key, "key");
        return Optional.ofNullable(invocations.get(key)).map(Registration::handler);
    }

    /**
     * Looks up the declared payload type of a local handler.
     *
     * @param key the stable identity of the job
     * @return the payload class, or empty when no payload type is registered
     */
    public Optional<Class<?>> payloadType(JobKey key) {
        Objects.requireNonNull(key, "key");
        return Optional.ofNullable(invocations.get(key)).map(Registration::payloadType);
    }

    private record Registration(JobHandler handler, @Nullable Class<?> payloadType) {
    }
}
