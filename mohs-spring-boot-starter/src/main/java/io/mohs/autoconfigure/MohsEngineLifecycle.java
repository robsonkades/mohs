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

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import org.jspecify.annotations.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

import io.mohs.core.EngineState;
import io.mohs.core.MohsLifecycle;
import io.mohs.core.definition.JobDefinition;
import io.mohs.engine.JobStore;
import io.mohs.engine.RetryPolicyRegistry;
import io.mohs.engine.StoredJob;

/**
 * Adapts {@link MohsLifecycle} (the domain's {@code start()}/{@code stop(Duration)}) to
 * {@link SmartLifecycle} (Spring's {@code start()}/{@code stop()}) — similarly shaped interfaces
 * with incompatible signatures, not the same thing under two names.
 *
 * <p>Late phase ({@link SmartLifecycle#DEFAULT_PHASE}): it starts last, after any job-registration
 * bean that may come to exist, because no claim may happen before every annotated definition is
 * registered — and it stops first.
 */
final class MohsEngineLifecycle implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(MohsEngineLifecycle.class);

    private final MohsLifecycle engine;
    private final boolean autoStartup;
    private final Duration shutdownGracePeriod;
    private final JobStore jobStore;
    private final RetryPolicyRegistry retryPolicies;
    private final @Nullable Duration watchdogTimeout;

    MohsEngineLifecycle(MohsLifecycle engine, boolean autoStartup, Duration shutdownGracePeriod, JobStore jobStore,
            @Nullable Duration watchdogTimeout, RetryPolicyRegistry retryPolicies) {
        this.engine = Objects.requireNonNull(engine, "engine");
        this.autoStartup = autoStartup;
        this.shutdownGracePeriod = Objects.requireNonNull(shutdownGracePeriod, "shutdownGracePeriod");
        this.jobStore = Objects.requireNonNull(jobStore, "jobStore");
        this.watchdogTimeout = watchdogTimeout;
        this.retryPolicies = Objects.requireNonNull(retryPolicies, "retryPolicies");
    }

    @Override
    public void start() {
        checkDeclaredPolicies();
        engine.start();
    }

    /**
     * Boot-time warnings about gaps between what a definition declares and what the engine
     * delivers — the operator needs to know the price at boot, not in the postmortem. Two checks,
     * over a single pass of the store:
     * <ol>
     *   <li>with per-node liveness, a healthy slow handler is no longer reclaimed; the remaining
     *       risk is the Watchdog Bound ({@code mohs.engine.watchdog-timeout}) being smaller than
     *       the job's declared {@code timeout} — the node would release ownership before the
     *       deadline the job gave itself, failing a still-healthy execution;</li>
     *   <li>a job naming a {@code retryPolicy} bean that does not exist — the execution would fail
     *       with the built-in backoff, indistinguishable from the custom policy having chosen it.</li>
     * </ol>
     *
     * <p>Diagnostics never bring the boot down: a read failure becomes a WARN carrying the full
     * cause, and the engine starts regardless.
     */
    private void checkDeclaredPolicies() {
        List<String> missingPolicies = new ArrayList<>();
        try (Stream<StoredJob> jobs = jobStore.findAll()) {
            jobs.forEach(stored -> {
                warnIfTimeoutOutlivesWatchdogBound(stored.definition());
                collectMissingRetryPolicy(stored, missingPolicies);
            });
        } catch (RuntimeException e) {
            log.warn("could not check declared job policies on startup", e);
        }
        if (!missingPolicies.isEmpty()) {
            throw new IllegalStateException("job(s) declare a retryPolicy bean that does not exist: "
                    + String.join("; ", missingPolicies)
                    + " — declare the RetryPolicy bean under that name, or drop the attribute to use the built-in "
                    + "exponential backoff with full jitter");
        }
    }

    private void warnIfTimeoutOutlivesWatchdogBound(JobDefinition definition) {
        if (watchdogTimeout != null && definition.timeout() != null && definition.timeout().compareTo(watchdogTimeout) >= 0) {
            log.warn(
                    "job '{}' declares timeout {} >= mohs.engine.watchdog-timeout {} — the watchdog releases ownership "
                            + "before the job's own deadline, failing a still-healthy run (retry budget applies); "
                            + "raise mohs.engine.watchdog-timeout above the slowest declared job timeout",
                    definition.key().value(), definition.timeout(), watchdogTimeout);
        }
    }

    /**
     * An ORPHANED definition is excluded on purpose: its annotation is gone from the code, so the
     * bean that served it is legitimately gone too — failing the boot over a job that can no longer
     * run would make removing a job a breaking change.
     */
    private void collectMissingRetryPolicy(StoredJob stored, List<String> missing) {
        String policy = stored.definition().retryPolicy();
        if (policy != null && !stored.orphaned() && !retryPolicies.contains(policy)) {
            missing.add("'" + stored.definition().key().value() + "' -> retryPolicy '" + policy + "'");
        }
    }

    @Override
    public void stop() {
        engine.stop(shutdownGracePeriod);
    }

    @Override
    public boolean isRunning() {
        EngineState state = engine.state();
        return state != EngineState.CREATED && state != EngineState.STOPPED;
    }

    @Override
    public boolean isAutoStartup() {
        return autoStartup;
    }

    /**
     * Explicit even though it matches the interface default: the phase is a documented
     * architectural guarantee — start last, stop first — not a coincidence of defaults.
     */
    @Override
    public int getPhase() {
        return SmartLifecycle.DEFAULT_PHASE;
    }
}
