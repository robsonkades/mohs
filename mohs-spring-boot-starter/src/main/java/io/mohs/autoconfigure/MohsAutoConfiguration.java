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

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import tools.jackson.databind.json.JsonMapper;

import io.mohs.core.Mohs;
import io.mohs.core.event.ExecutionInterceptor;
import io.mohs.core.event.ExecutionListener;
import io.mohs.core.execution.RetryPolicy;
import io.mohs.core.resource.ExecutionWindow;
import io.mohs.core.resource.MohsRunner;
import io.mohs.core.resource.RateLimit;
import io.mohs.engine.BatchCompletionCallbacks;
import io.mohs.engine.BatchStore;
import io.mohs.engine.CompletionBatcher;
import io.mohs.engine.Dispatcher;
import io.mohs.engine.Engine;
import io.mohs.engine.EngineMetrics;
import io.mohs.engine.EngineSettings;
import io.mohs.engine.ExecutionWindowRegistry;
import io.mohs.engine.HandlerRegistry;
import io.mohs.engine.HistoryStore;
import io.mohs.engine.JobStore;
import io.mohs.engine.LeaseStore;
import io.mohs.engine.MohsExecutors;
import io.mohs.engine.MohsImpl;
import io.mohs.engine.NodeStore;
import io.mohs.engine.RateLimitStore;
import io.mohs.engine.RetryPolicyRegistry;
import io.mohs.engine.RunnerRegistry;
import io.mohs.engine.StoreTransactions;
import io.mohs.engine.TriggerFirer;
import io.mohs.engine.WorkQueue;
import io.mohs.store.jdbc.DatabaseClock;
import io.mohs.store.jdbc.JdbcBatchStore;
import io.mohs.store.jdbc.JdbcHistoryStore;
import io.mohs.store.jdbc.JdbcJobStore;
import io.mohs.store.jdbc.JdbcLeaseStore;
import io.mohs.store.jdbc.JdbcNodeStore;
import io.mohs.store.jdbc.JdbcRateLimitStore;
import io.mohs.store.jdbc.JdbcStoreTransactions;
import io.mohs.store.jdbc.JdbcTriggerFirer;
import io.mohs.store.jdbc.JdbcWorkQueue;
import io.mohs.store.jdbc.MohsFlyway;
import io.mohs.store.jdbc.dialect.H2JdbcDialect;
import io.mohs.store.jdbc.dialect.JdbcDialect;
import io.mohs.store.jdbc.dialect.MySqlJdbcDialect;
import io.mohs.store.jdbc.dialect.PostgresJdbcDialect;
import io.mohs.store.jdbc.dialect.SqlServerJdbcDialect;

/**
 * Wires the engine ({@code io.mohs.engine}/{@code io.mohs.store.jdbc}) to a real Spring Boot
 * {@link DataSource}.
 *
 * <p>This package is free to depend on internals — {@code ArchitectureTest.PUBLIC_API} excludes
 * {@code io.mohs.autoconfigure} from the packages barred from seeing {@code io.mohs.engine} and
 * {@code io.mohs.store.jdbc}, because that is precisely this package's job. Scanning for
 * {@code @MohsJob} ({@link MohsJobScanner}) and named runners ({@link RunnerRegistry}) are assembled
 * here.
 *
 * <p>The {@link ThreadPoolTaskScheduler} for {@link DatabaseClock}'s resync (the {@link Engine}
 * loop is a thread of its own, not a bean) and the event {@link AsyncTaskExecutor} coexist with the
 * host application's own executors (for instance {@code applicationTaskExecutor}), so every
 * injection point carries an explicit {@link Qualifier} rather than relying on Spring's
 * resolve-by-name fallback — explicit code where magic would also work.
 *
 * <p>Every bean of a generic framework type ({@link Clock}, {@link ThreadPoolTaskScheduler},
 * {@link AsyncTaskExecutor}) is {@code defaultCandidate = false}: they are the engine's internal
 * infrastructure, not API shared with the host. Without that, merely having Mohs on the classpath
 * would suppress the application's auto-configured {@code taskScheduler}/
 * {@code applicationTaskExecutor} — Boot's conditions are {@code @ConditionalOnMissingBean} by
 * type, and this auto-configuration is ordered before Boot's — and a second {@link Clock} in the
 * context would break unqualified injection the application already had. That is silent degradation
 * of the host. Internal injection points keep working through {@link Qualifier}.
 *
 * <p>No bean here backs off with {@code @ConditionalOnMissingBean}, and that is deliberate rather
 * than an oversight: internal infrastructure is not an extension point. The host's surface is the
 * {@code io.mohs.core} vocabulary collected as beans, plus the validated properties.
 */
@AutoConfiguration(after = DataSourceAutoConfiguration.class)
@ConditionalOnProperty(prefix = "mohs", name = "enabled", matchIfMissing = true)
@EnableConfigurationProperties(MohsProperties.class)
public class MohsAutoConfiguration {

    private static final Logger log = LoggerFactory.getLogger(MohsAutoConfiguration.class);

    @Bean
    public JdbcDialect mohsJdbcDialect(MohsProperties properties) {
        MohsProperties.Jdbc.Dialect dialect = properties.jdbc().dialect();
        if (dialect == null) {
            throw new IllegalStateException(
                    "mohs.jdbc.dialect must be set (h2, postgresql, mysql or sqlserver) — "
                            + "the JDBC dialect is never auto-detected from the DataSource");
        }
        // H2 is a test/dev-only tier. Its SKIP LOCKED has a real, measured race (~33% double-lock,
        // see JdbcWorkQueue's Javadoc); claim correctness comes from the guarded CAS, but nobody
        // should discover that in production. A WARN rather than an error: the demo and the dev
        // loop depend on it on purpose.
        if (dialect == MohsProperties.Jdbc.Dialect.H2) {
            log.warn("mohs.jdbc.dialect=h2: H2 is Tier 3 — a test/dev backend, NOT supported in production");
        }
        return switch (dialect) {
            case H2 -> new H2JdbcDialect();
            case POSTGRESQL -> new PostgresJdbcDialect();
            case MYSQL -> new MySqlJdbcDialect();
            case SQLSERVER -> new SqlServerJdbcDialect();
        };
    }

    /**
     * The "database mode" condition has a single source: the two slices are mutually exclusive and
     * exhaustive over {@code mohs.time.mode} (an invalid value never reaches here — it fails
     * earlier, when binding the {@link MohsProperties.Time.Mode} enum), and the resync scheduler
     * exists only in the slice that uses it.
     */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnProperty(prefix = "mohs.time", name = "mode", havingValue = "database")
    static class DatabaseTimeConfiguration {

        @Bean(defaultCandidate = false)
        @Qualifier("mohsClockSyncScheduler")
        ThreadPoolTaskScheduler mohsClockSyncScheduler() {
            return MohsExecutors.scheduler("mohs-clock-sync", 1);
        }

        /**
         * Synchronises once at boot — a deliberate block, because the {@link Engine} must not start
         * with an unsynchronised clock — and then schedules the resync (see
         * {@link io.mohs.engine.SyncableClock}'s Javadoc).
         */
        @Bean(defaultCandidate = false)
        @Qualifier("mohsClock")
        Clock mohsClock(MohsProperties properties, DataSource dataSource, @Qualifier("mohsClockSyncScheduler") ThreadPoolTaskScheduler mohsClockSyncScheduler) {
            rejectDatabaseTimeOnZonelessDialects(properties.jdbc().dialect());
            DatabaseClock clock = new DatabaseClock(dataSource, properties.time().skewWarnThreshold());
            clock.sync();
            mohsClockSyncScheduler.scheduleWithFixedDelay(clock::sync, properties.time().syncInterval());
            return clock;
        }

        /**
         * Where {@code CURRENT_TIMESTAMP} carries no zone, {@code DatabaseClock} samples the distance
         * between two ZONES instead of between two clocks — and does it silently. A node in a zone
         * other than the database server's would then schedule, claim and expire leases hours away
         * from the rest of the cluster, with no error anywhere.
         *
         * <p>Both listed dialects have that shape: SQL Server's {@code DATETIME} is zoneless, and
         * MySQL's is evaluated in the session's {@code time_zone} and materialised by Connector/J in
         * the JVM's default zone. PostgreSQL and H2 answer {@code TIMESTAMPTZ} and are unaffected.
         *
         * <p>A boot failure instead of a WARN, unlike the H2 tier: H2 works and is merely unsupported,
         * whereas this is a wrong answer to "what time is it" in a component whose entire job is
         * knowing that. And the alternative it names is the default, so the failure costs nothing but
         * a property. The real fix — a per-dialect now-query ({@code SYSUTCDATETIME()},
         * {@code UTC_TIMESTAMP()}) — is a behaviour change to the clock and belongs to its own
         * decision; until then the trap is closed rather than hidden.
         */
        private static void rejectDatabaseTimeOnZonelessDialects(MohsProperties.Jdbc.@Nullable Dialect dialect) {
            if (dialect == MohsProperties.Jdbc.Dialect.SQLSERVER || dialect == MohsProperties.Jdbc.Dialect.MYSQL) {
                throw new IllegalStateException(
                        "mohs.time.mode=database is not supported on " + dialect + ": CURRENT_TIMESTAMP is zoneless "
                                + "there, so the sampled offset would be the distance between two ZONES, not between "
                                + "two clocks. Use mohs.time.mode=application (the default) and keep the hosts on NTP.");
            }
        }
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnProperty(prefix = "mohs.time", name = "mode", havingValue = "application", matchIfMissing = true)
    static class SystemTimeConfiguration {

        @Bean(defaultCandidate = false)
        @Qualifier("mohsClock")
        Clock mohsClock() {
            return Clock.systemUTC();
        }
    }

    /**
     * Mohs's migrations run when THIS bean is created, and the ordering is guaranteed by the
     * dependency GRAPH rather than by registration order: every bean that touches a Mohs table
     * (stores, claimer, reaper, trigger firer) takes {@code MohsFlyway} as a parameter. A host bean
     * that injects {@code Mohs} and writes in its constructor forces the whole chain and still
     * passes through here first — Mohs's own writers were already late by construction (the scanner
     * and registrar are {@code afterSingletonsInstantiated}, the engine is a
     * {@code SmartLifecycle}), but the host had no edge at all.
     *
     * <p>The bean is always present; {@code mohs.jdbc.migrate=false} only skips the
     * {@code migrate()}. It keeps its own instance and history table
     * ({@code mohs_schema_history}) — see {@link MohsFlyway}'s Javadoc on why never the host's
     * Flyway.
     */
    @Bean
    public MohsFlyway mohsFlyway(DataSource dataSource, JdbcDialect mohsJdbcDialect, MohsProperties properties) {
        MohsFlyway flyway = new MohsFlyway(dataSource, mohsJdbcDialect);
        if (properties.jdbc().migrate()) {
            flyway.migrate();
        }
        return flyway;
    }

    @Bean
    public JobStore mohsJobStore(DataSource dataSource, @Qualifier("mohsClock") Clock mohsClock, MohsFlyway mohsFlyway) {
        return new JdbcJobStore(dataSource, mohsClock);
    }

    /**
     * The raw {@code JsonMapper} is deliberate: the persisted payload format belongs to Mohs, not
     * to the host's web configuration. Switching to the context's {@code ObjectMapper} would let
     * the application's HTTP configuration define a durable format shared between nodes, and would
     * break reading already-written payloads the day it changed.
     */
    @Bean
    public HistoryStore mohsHistoryStore(DataSource dataSource, JdbcDialect mohsJdbcDialect, MohsFlyway mohsFlyway) {
        return new JdbcHistoryStore(dataSource, JsonMapper.builder().build(), mohsJdbcDialect);
    }

    @Bean
    public WorkQueue mohsWorkQueue(DataSource dataSource, JdbcDialect mohsJdbcDialect, BatchStore mohsBatchStore, MohsFlyway mohsFlyway) {
        return new JdbcWorkQueue(dataSource, mohsJdbcDialect, mohsBatchStore);
    }

    @Bean
    public LeaseStore mohsLeaseStore(DataSource dataSource, JdbcDialect mohsJdbcDialect, BatchStore mohsBatchStore, MohsFlyway mohsFlyway) {
        return new JdbcLeaseStore(dataSource, mohsJdbcDialect, mohsBatchStore);
    }

    /** The transactional boundary of the enqueue unit — REQUIRED: it joins the host's transaction when there is one, or opens its own. */
    @Bean
    public StoreTransactions mohsStoreTransactions(DataSource dataSource, MohsFlyway mohsFlyway) {
        return new JdbcStoreTransactions(dataSource);
    }

    @Bean
    public NodeStore mohsNodeStore(DataSource dataSource, MohsFlyway mohsFlyway) {
        return new JdbcNodeStore(dataSource);
    }

    @Bean(defaultCandidate = false)
    @Qualifier("mohsEventExecutor")
    public AsyncTaskExecutor mohsEventExecutor(MohsProperties properties) {
        return MohsExecutors.ioBoundExecutor("mohs-events", properties.engine().eventConcurrency());
    }

    /** Defaults built-in, overrides e conflito de fonte: {@link MohsRunners#assemble}. */
    @Bean(destroyMethod = "close")
    public RunnerRegistry mohsRunnerRegistry(MohsProperties properties, List<MohsRunner> mohsRunnerBeans) {
        return new RunnerRegistry(MohsRunners.assemble(properties, mohsRunnerBeans));
    }

    @Bean
    public RateLimitStore mohsRateLimitStore(DataSource dataSource, @Qualifier("mohsClock") Clock mohsClock, MohsFlyway mohsFlyway) {
        return new JdbcRateLimitStore(dataSource, mohsClock);
    }

    /**
     * Declared limits are registered AFTER all singletons, exactly as {@link MohsJobScanner} does
     * with definitions: writing to the database at boot is only safe once the schema exists, and
     * what guarantees that is Mohs's own Flyway ({@link #mohsFlyway}), created before any singleton
     * that touches the tables. Assembly, by contrast, runs when the bean is created — a malformed
     * property brings the boot down early, naming the property that is missing.
     */
    @Bean
    public SmartInitializingSingleton mohsRateLimitRegistrar(RateLimitStore mohsRateLimitStore, MohsProperties properties,
            List<RateLimit> mohsRateLimitBeans) {
        List<RateLimit> declared = MohsRateLimits.assemble(properties, mohsRateLimitBeans);
        return () -> MohsRateLimits.register(mohsRateLimitStore, properties.registration().onConflict(), declared);
    }

    /** No property-based path — {@link ExecutionWindow} exists only through a {@code @Bean} (see the class Javadoc). */
    @Bean
    public ExecutionWindowRegistry mohsExecutionWindowRegistry(List<ExecutionWindow> mohsExecutionWindowBeans) {
        return new ExecutionWindowRegistry(mohsExecutionWindowBeans);
    }

    @Bean
    public TriggerFirer mohsTriggerFirer(DataSource dataSource, HistoryStore mohsHistoryStore, WorkQueue mohsWorkQueue, MohsFlyway mohsFlyway) {
        return new JdbcTriggerFirer(dataSource, mohsHistoryStore, mohsWorkQueue);
    }

    /** Born empty — {@link MohsJobScanner} populates it in {@code afterSingletonsInstantiated}, before the {@link Engine} starts. */
    @Bean
    public HandlerRegistry mohsHandlerRegistry() {
        return new HandlerRegistry();
    }

    /**
     * Metrics are always on. A host with Micrometer in the context (actuator) sees everything under
     * {@code mohs.*}; with no registry, a local {@link SimpleMeterRegistry} keeps the engine
     * identical — inert for the host, and with no conditional path in the hot code.
     */
    @Bean
    public EngineMetrics mohsEngineMetrics(ObjectProvider<MeterRegistry> meterRegistry) {
        return new EngineMetrics(meterRegistry.getIfAvailable(SimpleMeterRegistry::new));
    }

    @Bean
    public Dispatcher mohsDispatcher(
            LeaseStore mohsLeaseStore,
            JobStore mohsJobStore,
            HandlerRegistry mohsHandlerRegistry,
            @Qualifier("mohsClock") Clock mohsClock,
            List<ExecutionInterceptor> interceptors,
            List<ExecutionListener> listeners,
            @Qualifier("mohsEventExecutor") AsyncTaskExecutor mohsEventExecutor,
            EngineMetrics mohsEngineMetrics,
            ObjectProvider<CompletionBatcher> mohsCompletionBatcher,
            RetryPolicyRegistry mohsRetryPolicyRegistry
    ) {
        return new Dispatcher(mohsLeaseStore, mohsJobStore, mohsHandlerRegistry, mohsClock, interceptors, listeners,
                mohsEventExecutor, mohsEngineMetrics, mohsCompletionBatcher.getIfAvailable(), mohsRetryPolicyRegistry);
    }

    /**
     * The declared {@link io.mohs.core.execution.RetryPolicy} beans, BY BEAN NAME — which is how a
     * job names one, and the reason this injection point is a {@code Map} rather than a {@code List}
     * (Spring fills the keys with the bean names).
     */
    @Bean
    public RetryPolicyRegistry mohsRetryPolicyRegistry(Map<String, RetryPolicy> retryPolicies) {
        return new RetryPolicyRegistry(retryPolicies);
    }

    /**
     * Group commit for completions — N=256/T=5ms, fixed by decision, the only knob being the
     * opt-out. {@code start}/{@code close} follow the context lifecycle: the {@code SmartLifecycle}
     * in {@link #mohsEngineLifecycle} stops the engine BEFORE beans are destroyed, so the close
     * drains what the last handlers submitted.
     */
    @Bean(initMethod = "start", destroyMethod = "close")
    @ConditionalOnProperty(name = "mohs.engine.completion-flush-on-every-result", havingValue = "false", matchIfMissing = true)
    public CompletionBatcher mohsCompletionBatcher(LeaseStore mohsLeaseStore, JobStore mohsJobStore) {
        return new CompletionBatcher(mohsLeaseStore, mohsJobStore, 256, Duration.ofMillis(5));
    }

    @Bean
    public Engine mohsEngine(
            WorkQueue mohsWorkQueue,
            Dispatcher mohsDispatcher,
            HistoryStore mohsHistoryStore,
            LeaseStore mohsLeaseStore,
            JobStore mohsJobStore,
            NodeStore mohsNodeStore,
            TriggerFirer mohsTriggerFirer,
            ExecutionWindowRegistry mohsExecutionWindowRegistry,
            RateLimitStore mohsRateLimitStore,
            @Qualifier("mohsClock") Clock mohsClock,
            MohsProperties properties,
            RunnerRegistry mohsRunnerRegistry,
            EngineMetrics mohsEngineMetrics,
            RetryPolicyRegistry mohsRetryPolicyRegistry
    ) {
        MohsProperties.Engine engineProperties = properties.engine();
        EngineSettings settings = new EngineSettings(engineProperties.pollInterval(), engineProperties.maxPollInterval(),
                engineProperties.batchSize(), engineProperties.dispatchConcurrency(), engineProperties.claimRounds(),
                engineProperties.leaseTtl(), engineProperties.nodeLeaseTtl(), engineProperties.watchdogTimeout(),
                engineProperties.misfireThreshold(), engineProperties.idempotencyRetention());
        return new Engine(mohsWorkQueue, mohsDispatcher, mohsHistoryStore, mohsLeaseStore, mohsJobStore, mohsNodeStore,
                mohsTriggerFirer, mohsExecutionWindowRegistry, mohsRateLimitStore, mohsClock, settings,
                mohsRunnerRegistry, mohsEngineMetrics, mohsRetryPolicyRegistry);
    }

    /** {@link SmartLifecycle} — see {@link MohsEngineLifecycle}'s Javadoc on the adaptation and on the lease-versus-timeout WARN. */
    @Bean
    public SmartLifecycle mohsEngineLifecycle(Engine mohsEngine, MohsProperties properties, JobStore mohsJobStore,
            RetryPolicyRegistry mohsRetryPolicyRegistry) {
        boolean autoStartup = properties.lifecycle().startMode() == MohsProperties.Lifecycle.StartMode.AUTO;
        return new MohsEngineLifecycle(mohsEngine, autoStartup, properties.lifecycle().shutdown().gracePeriod(),
                mohsJobStore, properties.engine().watchdogTimeout(), mohsRetryPolicyRegistry);
    }

    @Bean
    public BatchStore mohsBatchStore(DataSource dataSource, @Qualifier("mohsClock") Clock mohsClock, MohsFlyway mohsFlyway) {
        return new JdbcBatchStore(dataSource, mohsClock);
    }

    /**
     * The delivery side of {@code @OnExecution}: it joins the {@code List<ExecutionListener>} the
     * dispatcher already publishes to, so an annotated method is a listener in every respect that
     * matters — including being asynchronous and best-effort. It is created empty; the scanner fills
     * it in its second phase, still before the engine's {@code SmartLifecycle} starts.
     */
    @Bean
    public OnExecutionRegistry mohsOnExecutionRegistry() {
        return new OnExecutionRegistry();
    }

    /**
     * Joins {@link #mohsDispatcher}'s {@code ExecutionListener} list like any other: that is how
     * {@code Batch.onCompletion} receives the {@code BatchCompleted} the dispatcher publishes, with
     * no parallel delivery path.
     */
    @Bean
    public BatchCompletionCallbacks mohsBatchCompletionCallbacks() {
        return new BatchCompletionCallbacks();
    }

    @Bean
    public Mohs mohs(JobStore mohsJobStore, WorkQueue mohsWorkQueue, HistoryStore mohsHistoryStore,
            LeaseStore mohsLeaseStore, StoreTransactions mohsStoreTransactions, NodeStore mohsNodeStore,
            RateLimitStore mohsRateLimitStore, HandlerRegistry mohsHandlerRegistry,
            @Qualifier("mohsClock") Clock mohsClock, Engine mohsEngine, BatchStore mohsBatchStore,
            BatchCompletionCallbacks mohsBatchCompletionCallbacks, RunnerRegistry mohsRunnerRegistry) {
        return new MohsImpl(mohsJobStore, mohsWorkQueue, mohsHistoryStore, mohsLeaseStore, mohsStoreTransactions,
                mohsNodeStore, mohsRateLimitStore, mohsHandlerRegistry,
                mohsClock, mohsEngine, mohsBatchStore, mohsBatchCompletionCallbacks, mohsRunnerRegistry,
                mohsEngine::signalWorkScheduled);
    }

    /**
     * {@code static}: a {@code BeanPostProcessor} declared through a non-static {@code @Bean} risks
     * initialising this configuration class too early — a warning Spring itself emits — and
     * {@code static} avoids that without giving up ordinary autowired parameters.
     * {@code ObjectProvider} on all three parameters for the same reason as on
     * {@link MohsJobScanner}'s side: see its class Javadoc.
     */
    @Bean
    public static MohsJobScanner mohsJobScanner(
            ObjectProvider<HandlerRegistry> mohsHandlerRegistry,
            ObjectProvider<JobStore> mohsJobStore,
            ObjectProvider<MohsProperties> properties,
            ObjectProvider<OnExecutionRegistry> mohsOnExecutionRegistry
    ) {
        return new MohsJobScanner(mohsHandlerRegistry, mohsJobStore, properties, mohsOnExecutionRegistry);
    }
}
