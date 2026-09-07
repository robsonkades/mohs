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
import java.util.Map;

import org.jspecify.annotations.Nullable;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.boot.context.properties.bind.ConstructorBinding;

import io.mohs.core.resource.RunnerMode;
import io.mohs.rest.ApiPaths;

/**
 * The {@code mohs.*} properties — only what {@link MohsAutoConfiguration}, {@link MohsJobScanner}
 * and {@link MohsRestAutoConfiguration} actually consume so far (engine bean wiring, scanning for
 * {@code @MohsJob}, named runners and the v1 REST surface for jobs and executions).
 *
 * <p>Records with constructor binding: a property is an immutable snapshot of the boot, not mutable
 * state. Component documentation goes in the {@code @param} tags, since that is what the
 * configuration processor reads to generate record metadata.
 *
 * @param enabled master gate — turning it off removes every Mohs bean from the context
 * @param runners named runners in addition to the built-in ones — see {@link Runner}
 * @param rateLimits cluster-wide throughput limits by name ({@code mohs.rate-limits.smtp.max=100}) — see {@link RateLimitSpec}
 * @param jdbc the JDBC configuration
 * @param engine the local execution engine
 * @param lifecycle the startup and shutdown settings
 * @param time the clock synchronization settings
 * @param registration the startup definition reconciliation settings
 * @param api the operational REST API settings
 */
@ConfigurationProperties("mohs")
public record MohsProperties(
        @DefaultValue("true") boolean enabled,
        @DefaultValue Jdbc jdbc,
        @DefaultValue Engine engine,
        @DefaultValue Lifecycle lifecycle,
        @DefaultValue Time time,
        @DefaultValue Registration registration,
        @DefaultValue Api api,
        @DefaultValue Map<String, Runner> runners,
        @DefaultValue Map<String, RateLimitSpec> rateLimits) {

    /**
     * Mohs does NOT create or migrate its schema: the operator applies {@code schema-<dialect>.sql}
     * before the application starts, and the versioned {@code V*.sql} files in the jar are the deltas
     * for an upgrade. There is no {@code migrate} switch because there is nothing to switch off.
     *
     * @param dialect an explicit choice, never auto-detected from the {@code DataSource}. No default — mandatory.
     */
    public record Jdbc(@Nullable Dialect dialect) {

        /**
         * The explicitly selected SQL dialect.
         */
        public enum Dialect {
            /**
             * The H2 SQL dialect.
             */
            H2,
            /** The PostgreSQL SQL dialect. */
            POSTGRESQL,
            /** The MySQL SQL dialect. */
            MYSQL,
            /** The Microsoft SQL Server dialect. */
            SQLSERVER
        }
    }

    /**
     * Configures polling, execution ownership, dispatch and retention.
     *
     * @param pollInterval the FLOOR of the interval between engine loop ticks: the loop polls at this rate while it keeps finding work and doubles the interval on every empty tick up to {@code max-poll-interval}; default 25ms — dispatch latency is about poll/2 in the worst case without a wake-up, and idle cost is controlled by the backoff, not by the floor
     * @param maxPollInterval the CEILING of the adaptive backoff: the longest interval between ticks of an idle engine; must be >= {@code poll-interval}; the actual sleep never exceeds {@code node-lease-ttl/3} — the heartbeat has its own cadence, and a high ceiling must not let an idle node be declared dead
     * @param batchSize the maximum number of executions claimed per claim
     * @param claimRounds how many claims one tick chains while the batch keeps coming back full and dispatch has headroom — it loosens the coupling between throughput and {@code poll-interval} under backlog; 1 (the default) is the classic one-claim-per-tick shape
     * @param leaseTtl feeds {@code lease_expires_at} at claim time; it is also the staleness cutoff for a legacy node row with no {@code expires_at}
     * @param nodeLeaseTtl the NODE's lease — each tick's heartbeat promises "alive until now+TTL" in {@code mohs_nodes.expires_at}; the reaper only reclaims executions from a node whose promise has expired; a sanity floor of 12s is validated at boot — one tick sleeps up to a third of the TTL and then spends up to 7s on the idempotency prune and the queue-depth count (plus up to ~5s on the hourly history sweep, when one is enabled — a 2s budget checked between passes, and a pass is up to three 1s-capped statements), and 12s is the smallest promise that still outlasts the always-on steps with a second of margin for clock skew
     * @param watchdogTimeout the Watchdog Bound: a runtime ceiling — on reaching it the node RELEASES ownership (a fenced failure, with normal retry); {@code null} (the default) means no ceiling; when present it must be greater than {@code node-lease-ttl} (validated while assembling the engine)
     * @param misfireThreshold separates a late firing from a lost one — an occurrence due within the threshold fires late under any policy; anything older answers to the job's {@code Misfire}
     * @param idempotencyRetention how long an {@code Idempotency-Key} keeps deduplicating — it IS the window, because the key deduplicates for exactly as long as its row lives in {@code mohs_idempotency}; the engine prunes older rows hourly, and {@code 0s} turns pruning off and keeps every key forever (an unbounded table)
     * @param historyRetention how long a TERMINAL execution's history (its row, its attempts, and a batch none of whose members remain) survives after finishing; {@code 0s} — the default — keeps everything forever, because deleting history is the operator's decision and never the scheduler's surprise; a positive window is enforced hourly, in bounded batches, and does not touch {@code mohs_idempotency} (that table answers to {@code idempotency-retention} alone)
     * @param dispatchConcurrency the real concurrency ceiling of the dispatch executor (never through pool size); it also bounds the claim
     * @param eventConcurrency the real concurrency ceiling of the event-publication executor
     * @param completionFlushOnEveryResult turns off group commit for completions and returns to a synchronous commit per result — it trades the durability window (~5ms) for the earlier per-execution latency; the only knob the decision adds
     */
    public record Engine(
            @DefaultValue("25ms") Duration pollInterval,
            @DefaultValue("2s") Duration maxPollInterval,
            @DefaultValue("50") int batchSize,
            @DefaultValue("1") int claimRounds,
            @DefaultValue("30s") Duration leaseTtl,
            @DefaultValue("15s") Duration nodeLeaseTtl,
            @Nullable Duration watchdogTimeout,
            @DefaultValue("60s") Duration misfireThreshold,
            @DefaultValue("7d") Duration idempotencyRetention,
            @DefaultValue("0s") Duration historyRetention,
            @DefaultValue("64") int dispatchConcurrency,
            @DefaultValue("16") int eventConcurrency,
            @DefaultValue("false") boolean completionFlushOnEveryResult) {
    }

    /**
     * Controls automatic startup and graceful shutdown.
     *
     * @param startMode {@code auto} calls {@link io.mohs.core.MohsLifecycle#start()} by itself at boot; {@code manual} waits for the consumer to call it
     * @param startupDelay one-time wait after start is requested, before any engine tick; nonnegative, default zero
     * @param shutdown the graceful shutdown settings
     */
    public record Lifecycle(
            @DefaultValue("auto") StartMode startMode,
            @DefaultValue Shutdown shutdown,
            @DefaultValue("0s") Duration startupDelay) {

        /**
         * Binds the canonical constructor even when the compatibility constructor is present.
         * @param startMode whether startup is automatic or manual
         * @param shutdown the graceful shutdown settings
         * @param startupDelay the one-time delay before processing
         */
        @ConstructorBinding
        public Lifecycle {
        }

        /**
         * Preserves immediate startup for existing programmatic configurations.
         * @param startMode whether startup is automatic or manual
         * @param shutdown the graceful shutdown settings
         */
        public Lifecycle(StartMode startMode, Shutdown shutdown) {
            this(startMode, shutdown, Duration.ZERO);
        }

        /**
         * Whether Spring starts the engine automatically.
         */
        public enum StartMode {
            /**
             * Starts the engine with the Spring application context.
             */
            AUTO,
            /** Waits for an explicit lifecycle start call. */
            MANUAL
        }

        /**
         * Bounds the wait for in-flight work during shutdown.
         *
         * @param gracePeriod how long shutdown waits for in-flight executions before interrupting them
         */
        public record Shutdown(@DefaultValue("30s") Duration gracePeriod) {
        }
    }

    /**
     * Selects the cluster time source and its synchronization cadence.
     *
     * @param mode {@code application} uses the system clock; {@code database} uses {@link io.mohs.store.jdbc.DatabaseClock} (the database is the cluster's time authority)
     * @param skewWarnThreshold only read when {@code mode} is {@code database} — the WARN threshold of {@link io.mohs.store.jdbc.DatabaseClock#sync()}
     * @param syncInterval only read when {@code mode} is {@code database} — how often to resample (see {@link io.mohs.engine.SyncableClock}'s Javadoc, which already names this property)
     */
    public record Time(
            @DefaultValue("application") Mode mode,
            @DefaultValue("1s") Duration skewWarnThreshold,
            @DefaultValue("30s") Duration syncInterval) {

        /**
         * The authority used for scheduler wall-clock time.
         */
        public enum Mode {
            /**
             * Uses the application system clock.
             */
            APPLICATION,
            /** Synchronizes scheduler time with the database clock. */
            DATABASE
        }
    }

    /**
     * Controls reconciliation of code and persisted job definitions at startup.
     *
     * @param onConflict how {@link MohsJobScanner} resolves definitional drift between the code and what is already in the store
     */
    public record Registration(@DefaultValue("override") OnConflict onConflict) {

        /**
         * The policy for differences between code and stored definitions.
         */
        public enum OnConflict {
            /** Code wins; every change is logged with a diff (the default). */
            OVERRIDE,
            /** The store wins; the code's version is ignored with a WARN. */
            PRESERVE,
            /** Drift brings the boot down, showing the diff. */
            FAIL
        }
    }

    /**
     * Closed by default ({@code enabled=false}) — turning it on is a conscious act, signalled by a
     * WARN at boot in {@link MohsRestAutoConfiguration}.
     *
     * @param enabled turns the operational REST API on
     * @param basePath the prefix of every {@code io.mohs.rest} route; the default is the same {@link ApiPaths#V1} constant used as the fallback of the {@code ${mohs.api.base-path:...}} placeholders in the {@code @RequestMapping}s (an annotation cannot read the binding — there the placeholder is the only mechanism; code reads this component)
     */
    public record Api(
            @DefaultValue("false") boolean enabled,
            @DefaultValue(ApiPaths.V1) String basePath) {
    }

    /**
     * A named runner in addition to the built-in {@code io}/{@code cpu} (assembled by
     * {@link MohsAutoConfiguration} with the documented defaults) — one value of
     * {@link MohsProperties#runners()}, the {@code Map} itself with no wrapper:
     * {@code mohs.runners.<name>.mode} plus the fields of the declared mode, the same shape as
     * the rule "Runners — a specification, never an Executor". A field
     * belonging to the wrong mode is a boot error during conversion to {@code MohsRunner}
     * ({@link MohsAutoConfiguration}) — the same stance as {@code MohsRunner} itself, which throws
     * for a wrong-mode field.
     *
     * <p>Spring's binder canonicalises a non-bracketed map key to lower case:
     * {@code mohs.runners.myUpload.*} registers the runner as {@code myupload}, and
     * {@code JobDefinition.runner()} is case-sensitive. Prefer lower-case names; to preserve exact
     * case, use the bracketed form ({@code mohs.runners.[myUpload].max=8}).
     *
     * @param mode {@code io} (the default) or {@code cpu} — decides which of the other fields apply
     * @param max {@link RunnerMode#IO} — defaults to 64 when omitted (the same default as {@code MohsRunner.IoBuilder})
     * @param coreSize {@link RunnerMode#CPU} — defaults to the available processors when omitted
     * @param maxSize {@link RunnerMode#CPU} — the pool's thread ceiling
     * @param queueCapacity {@link RunnerMode#CPU} — the pool's queue capacity
     * @param keepAlive {@link RunnerMode#CPU} — keep-alive for threads above the core size
     */
    public record Runner(
            @DefaultValue("io") RunnerMode mode,
            @Nullable Integer max,
            @Nullable Integer coreSize,
            @Nullable Integer maxSize,
            @Nullable Integer queueCapacity,
            @Nullable Duration keepAlive) {
    }

    /**
     * One value of {@code mohs.rate-limits.<name>}. The name is the map key (as in
     * {@link #runners()}), so it is not repeated here. Both fields are mandatory: a half-specified
     * limit has no defensible default — {@code max} without {@code window} is not a rate.
     *
     * @param max firings allowed per window, cluster-wide
     * @param window the window {@code max} applies over ({@code 1m}, {@code PT30S})
     */
    public record RateLimitSpec(@Nullable Integer max, @Nullable Duration window) {
    }
}
