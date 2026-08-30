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
package io.mohs.autoconfigure;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mohs.core.RateLimitSnapshot;
import io.mohs.core.resource.RateLimit;
import io.mohs.engine.RateLimitStore;

/**
 * Assembles the declared {@link RateLimit}s (beans plus properties) and registers them in the store
 * at boot — the equivalent of {@link MohsRunners} for the throughput axis, with one difference that
 * changes everything: a runner is node-local state, while a rate limit is SHARED state. So
 * registering here means writing to the database, and drift against what is already there follows
 * the {@code mohs.registration.on-conflict} policy, exactly like a job definition.
 *
 * <p>The token bucket is never touched by this path — what preserves the balance is
 * {@link RateLimitStore#upsert}: boot governs the spec, never the current state.
 */
final class MohsRateLimits {

    private static final Logger log = LoggerFactory.getLogger(MohsRateLimits.class);

    private MohsRateLimits() {
    }

    /**
     * A bean defines the structure and a property adjusts the numbers (the same contract as
     * for named resources): a name declared in both places keeps the
     * property's value, which is the one that can be changed without recompiling.
     */
    static List<RateLimit> assemble(MohsProperties properties, List<RateLimit> beanRateLimits) {
        Objects.requireNonNull(properties, "properties");
        Objects.requireNonNull(beanRateLimits, "beanRateLimits");

        Map<String, RateLimit> byName = new LinkedHashMap<>();
        for (RateLimit beanRateLimit : beanRateLimits) {
            RateLimit previous = byName.put(beanRateLimit.name(), beanRateLimit);
            if (previous != null) {
                throw new IllegalArgumentException("duplicate rate limit name '" + beanRateLimit.name()
                        + "' — two @Bean RateLimit declare the same name");
            }
        }
        properties.rateLimits().forEach((name, spec) -> byName.put(name, toRateLimit(name, spec)));
        return List.copyOf(byName.values());
    }

    /**
     * Both fields are mandatory, and a missing one brings the boot down naming the property that is
     * absent — a boot validation instead of a limit that would only reveal itself as wrong under
     * load.
     */
    private static RateLimit toRateLimit(String name, MohsProperties.RateLimitSpec spec) {
        if (spec.max() == null) {
            throw new IllegalArgumentException("rate limit '" + name + "' is missing mohs.rate-limits." + name + ".max");
        }
        if (spec.window() == null) {
            throw new IllegalArgumentException("rate limit '" + name + "' is missing mohs.rate-limits." + name + ".window");
        }
        return new RateLimit(name, spec.max(), spec.window());
    }

    /** Drift between what is declared and what is stored follows {@code on-conflict}, exactly like a job definition in {@link MohsJobScanner}. */
    static void register(RateLimitStore store, MohsProperties.Registration.OnConflict onConflict, List<RateLimit> declared) {
        for (RateLimit incoming : declared) {
            Optional<RateLimit> existing = store.find(incoming.name()).map(RateLimitSnapshot::rateLimit);
            if (existing.isEmpty() || existing.get().equals(incoming)) {
                store.upsert(incoming);
                continue;
            }
            switch (onConflict) {
                case OVERRIDE -> {
                    log.info("rate limit '{}' changed, code wins (mohs.registration.on-conflict=override): {} -> {}",
                            incoming.name(), describe(existing.get()), describe(incoming));
                    store.upsert(incoming);
                }
                case PRESERVE -> log.warn(
                        "rate limit '{}' changed but store wins (mohs.registration.on-conflict=preserve), code version ignored: {} kept over {}",
                        incoming.name(), describe(existing.get()), describe(incoming));
                case FAIL -> throw new IllegalStateException("rate limit '" + incoming.name()
                        + "' diverged from the stored one (mohs.registration.on-conflict=fail): "
                        + describe(existing.get()) + " stored, " + describe(incoming) + " declared");
            }
        }
    }

    private static String describe(RateLimit rateLimit) {
        return rateLimit.max() + "/" + rateLimit.window();
    }
}
