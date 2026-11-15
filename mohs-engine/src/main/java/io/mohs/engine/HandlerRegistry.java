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
import java.util.function.Consumer;
import io.mohs.core.definition.JobDefinition;
import io.mohs.core.job.JobKey;
import io.mohs.core.job.JobRef;
import org.jspecify.annotations.Nullable;

/** Local executable catalog. Persisted contracts are decoded only with a locally declared type. */
public final class HandlerRegistry {
    private final Map<JobKey, Registration> invocations = new ConcurrentHashMap<>();
    private final Consumer<JobDefinition> policyValidator;
    private @Nullable ExecutableCatalog frozen;

    public HandlerRegistry() { this(definition -> {}); }

    public HandlerRegistry(Consumer<JobDefinition> policyValidator) {
        this.policyValidator = Objects.requireNonNull(policyValidator, "policyValidator");
    }

    public void validatePolicy(JobDefinition definition) { policyValidator.accept(definition); }

    public void register(JobKey key, JobHandler handler, Class<?> payloadType) {
        register(key, handler, payloadType, key.value());
    }

    public synchronized void register(JobKey key, JobHandler handler, Class<?> payloadType, String inputContract) {
        if (frozen != null) throw new IllegalStateException("executable catalog is frozen; restart to change registrations");
        var reference = new JobRef<>(key, payloadType, inputContract);
        invocations.put(key, new Registration(Objects.requireNonNull(handler, "handler"), reference));
    }

    public Optional<JobHandler> find(JobKey key) {
        return Optional.ofNullable(invocations.get(Objects.requireNonNull(key, "key"))).map(Registration::handler);
    }

    public Optional<Class<?>> payloadType(JobKey key) {
        return Optional.ofNullable(invocations.get(Objects.requireNonNull(key, "key"))).map(r -> r.reference().payloadType());
    }

    public JobRef<?> reference(JobKey key) {
        var registration = invocations.get(Objects.requireNonNull(key, "key"));
        if (registration == null) throw new IllegalArgumentException("no registered executable '" + key.value() + "'");
        return registration.reference();
    }

    /** Freezes the local executable identities before publishing or joining the cluster. */
    public synchronized ExecutableCatalog freeze() {
        if (frozen == null) {
            var contracts = new java.util.HashMap<JobKey, String>();
            invocations.forEach((key, value) -> contracts.put(key, value.reference().inputContract()));
            frozen = new ExecutableCatalog(contracts);
        }
        return frozen;
    }

    public void validateInput(JobKey key, Object input, @Nullable Class<?> declaredType) {
        Class<?> expected = boxed(reference(key).payloadType());
        if (!expected.isInstance(Objects.requireNonNull(input, "input"))
                || declaredType != null && !expected.equals(boxed(declaredType))) {
            throw new IllegalArgumentException("job '" + key.value() + "' expects " + expected.getName()
                    + "; received " + input.getClass().getName());
        }
    }

    public void requireContract(JobKey key, String contract) {
        String expected = reference(key).inputContract();
        if (!expected.equals(contract)) throw new IllegalArgumentException("job '" + key.value()
                + "' expects input contract '" + expected + "'; received '" + contract + "'");
    }

    public JobPayload encode(JobKey key, Object input) { return InputCodec.JSON.encode(reference(key), input); }

    public Object decode(JobKey key, JobPayload input) { return InputCodec.JSON.decode(input, reference(key)); }

    private static Class<?> boxed(Class<?> type) { return org.springframework.util.ClassUtils.resolvePrimitiveIfNecessary(type); }
    private record Registration(JobHandler handler, JobRef<?> reference) {}
}
