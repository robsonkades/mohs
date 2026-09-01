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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
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
import io.mohs.store.jdbc.delegate.H2JdbcDelegate;
import io.mohs.store.jdbc.delegate.JdbcDelegate;
import io.mohs.store.jdbc.delegate.MySqlJdbcDelegate;
import io.mohs.store.jdbc.delegate.PostgresJdbcDelegate;
import io.mohs.store.jdbc.delegate.SqlServerJdbcDelegate;

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
 * <p>Exactly one bean here backs off with {@code @ConditionalOnMissingBean}, and the exception marks
 * the boundary rather than blurring it: {@link JdbcDelegate} is the one place a host is expected to
 * substitute, because it is what a database this repository does not ship needs. Everything else is
 * internal infrastructure and not an extension point — the host's surface is the
 * {@code io.mohs.core} vocabulary collected as beans, plus the validated properties.
 *
 * <p>Every injection point of that delegate names its parameter {@code delegate} — deliberately NOT
 * {@code mohsJdbcDelegate}, the bean's own name, and the divergence is load-bearing. A substitute
 * shipped in its OWN auto-configuration has no guaranteed ordering against this one, so both
 * delegates can reach the context; the build compiles with {@code -parameters}, and Spring resolves
 * that ambiguity by matching the parameter name against a bean name — under the bean's name it would
 * silently pick the built-in one and discard the substitute. Named anything else, the same context
 * fails at boot with {@code NoUniqueBeanDefinitionException}, which is the answer a host that ended up
 * with two delegates needs. Renaming them back for "consistency" restores the silent wrong choice.
 */
@AutoConfiguration(after = DataSourceAutoConfiguration.class)
@ConditionalOnProperty(prefix = "mohs", name = "enabled", matchIfMissing = true)
@EnableConfigurationProperties(MohsProperties.class)
public class MohsAutoConfiguration {

    private static final Logger log = LoggerFactory.getLogger(MohsAutoConfiguration.class);

    /**
     * The floor of {@code mohs.engine.node-lease-ttl}. The heartbeat goes out ONCE per tick, at the top
     * of it, and everything after it spends the promise it just made: the idempotency prune (5s), the
     * queue-depth count (2s), the claim sweep's own budget ({@code node-lease-ttl/4}, reachable across
     * up to 64 shard probes even with a single round, and reachable while claiming nothing), and last
     * the sleep ({@code node-lease-ttl/3}).
     *
     * <p>Summing every ceiling demands 17s and outlaws the 15s default this library ships — which is
     * the sign that the additive model is not a floor. Those ceilings only bite on a degraded database,
     * and there the same tick's UNBOUNDED queries (definitions, nodes, the reaper, the firing sweep,
     * each claim) blow the lease long before any minimum could help. So twelve is a SANITY floor,
     * chosen by MARGIN rather than by the sum: the healthy-tick budget {@code TTL/3 + 7s} outgrows the
     * promise only at 10.5s and below, and 11s clears it by a third of a second — twelve is the first
     * whole second that leaves a FULL one ({@code 4 + 7 < 12}), and that second is the margin for the
     * clock skew between nodes (a peer judges {@code expires_at} by its own clock) and for the
     * heartbeat's own write latency, since the promise is computed before the UPDATE that stores it.
     * Below the floor a node loses the lease it is renewing while alive and working, and its peers reap
     * what it is still running.
     */
    private static final Duration MIN_NODE_LEASE_TTL = Duration.ofSeconds(12);

    /**
     * The four databases this repository ships a delegate for, chosen by {@code mohs.jdbc.dialect}.
     *
     * <p>{@code @ConditionalOnMissingBean} is what makes a delegate written elsewhere possible: declare
     * a {@code JdbcDelegate} bean and it wins, with the property then unnecessary. "It wins" holds for a
     * bean in the application's own configuration, registered before any auto-configuration is
     * evaluated; one shipped inside another auto-configuration has no such guarantee and may land
     * alongside this one — which is why the injection points are named to fail rather than choose (see
     * the class Javadoc). A BEAN rather than a class name in a property — Quartz's
     * {@code driverDelegateClass} shape — because in a Spring application the container already builds
     * and injects it: reflection would buy nothing, and would cost a boot failure that arrives as a
     * stack trace instead of a compiler error.
     */
    @Bean
    @ConditionalOnMissingBean
    public JdbcDelegate mohsJdbcDelegate(MohsProperties properties) {
        MohsProperties.Jdbc.Dialect configuredDialect = properties.jdbc().dialect();
        if (configuredDialect == null) {
            throw new IllegalStateException(
                    "mohs.jdbc.dialect must be set (h2, postgresql, mysql or sqlserver) — the JDBC delegate is "
                            + "never auto-detected from the DataSource. For a database this repository does not "
                            + "ship, declare a JdbcDelegate bean instead and leave the property unset.");
        }
        // H2 is a test/dev-only tier. Its SKIP LOCKED has a real, measured race (~33% double-lock,
        // see JdbcWorkQueue's Javadoc); claim correctness comes from the guarded CAS, but nobody
        // should discover that in production. A WARN rather than an error: the demo and the dev
        // loop depend on it on purpose.
        if (configuredDialect == MohsProperties.Jdbc.Dialect.H2) {
            log.warn("mohs.jdbc.dialect=h2: H2 is Tier 3 — a test/dev backend, NOT supported in production");
        }
        return switch (configuredDialect) {
            case H2 -> new H2JdbcDelegate();
            case POSTGRESQL -> new PostgresJdbcDelegate();
            case MYSQL -> new MySqlJdbcDelegate();
            case SQLSERVER -> new SqlServerJdbcDelegate();
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
         *
         * <p>That first sample is the one failure this class refuses to absorb, and the asymmetry is
         * the point. {@code sync()} answers a failed sample with a WARN and keeps the last known
         * offset, which is right in steady state — a database blip must not stop an engine that is
         * already running. At boot there is no last known offset, so the same indulgence would start
         * the engine on the local clock the operator explicitly said not to trust, indistinguishable
         * from {@code mode=application} except for one WARN line. A boot that stops names its cause;
         * a boot that continues hands the cluster a disagreement about what time it is.
         */
        @Bean(defaultCandidate = false)
        @Qualifier("mohsClock")
        Clock mohsClock(MohsProperties properties, DataSource dataSource, JdbcDelegate delegate,
                @Qualifier("mohsClockSyncScheduler") ThreadPoolTaskScheduler mohsClockSyncScheduler) {
            MohsProperties.Time timeProperties = properties.time();
            DatabaseClock clock = new DatabaseClock(dataSource, timeProperties.skewWarnThreshold(), delegate);
            clock.sync();
            if (!clock.isSynchronised()) {
                throw new IllegalStateException(
                        "mohs.time.mode=database: the first clock sample against the database failed — see the "
                                + "preceding WARN for the cause. The engine will not start on an unsynchronised "
                                + "clock. Fix the database connectivity (or the delegate's nowQuery(), if it is one "
                                + "this repository does not ship), or set mohs.time.mode=application and keep the "
                                + "hosts on NTP.");
            }
            mohsClockSyncScheduler.scheduleWithFixedDelay(clock::sync, timeProperties.syncInterval());
            return clock;
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

    @Bean
    public JobStore mohsJobStore(DataSource dataSource, @Qualifier("mohsClock") Clock mohsClock, JdbcDelegate delegate) {
        return new JdbcJobStore(dataSource, mohsClock, delegate);
    }

    /**
     * The raw {@code JsonMapper} is deliberate: the persisted payload format belongs to Mohs, not
     * to the host's web configuration. Switching to the context's {@code ObjectMapper} would let
     * the application's HTTP configuration define a durable format shared between nodes, and would
     * break reading already-written payloads the day it changed.
     */
    @Bean
    public HistoryStore mohsHistoryStore(DataSource dataSource, JdbcDelegate delegate) {
        return new JdbcHistoryStore(dataSource, JsonMapper.builder().build(), delegate);
    }

    @Bean
    public WorkQueue mohsWorkQueue(DataSource dataSource, JdbcDelegate delegate, BatchStore mohsBatchStore) {
        return new JdbcWorkQueue(dataSource, delegate, mohsBatchStore);
    }

    @Bean
    public LeaseStore mohsLeaseStore(DataSource dataSource, JdbcDelegate delegate, BatchStore mohsBatchStore) {
        return new JdbcLeaseStore(dataSource, delegate, mohsBatchStore);
    }

    /** The transactional boundary of the enqueue unit — REQUIRED: it joins the host's transaction when there is one, or opens its own. */
    @Bean
    public StoreTransactions mohsStoreTransactions(DataSource dataSource) {
        return new JdbcStoreTransactions(dataSource);
    }

    @Bean
    public NodeStore mohsNodeStore(DataSource dataSource, JdbcDelegate delegate) {
        return new JdbcNodeStore(dataSource, delegate);
    }

    @Bean(defaultCandidate = false)
    @Qualifier("mohsEventExecutor")
    public AsyncTaskExecutor mohsEventExecutor(MohsProperties properties) {
        return MohsExecutors.ioBoundExecutor("mohs-events", properties.engine().eventConcurrency());
    }

    /** Built-in defaults, overrides and the name conflict between sources: {@link MohsRunners#assemble}. */
    @Bean(destroyMethod = "close")
    public RunnerRegistry mohsRunnerRegistry(MohsProperties properties, List<MohsRunner> mohsRunnerBeans) {
        return new RunnerRegistry(MohsRunners.assemble(properties, mohsRunnerBeans));
    }

    @Bean
    public RateLimitStore mohsRateLimitStore(DataSource dataSource, @Qualifier("mohsClock") Clock mohsClock, JdbcDelegate delegate) {
        return new JdbcRateLimitStore(dataSource, mohsClock, delegate);
    }

    /**
     * Declared limits are registered AFTER all singletons, exactly as {@link MohsJobScanner} does
     * with definitions: writing to the database at boot is only safe once the schema exists, and
     * since Mohs no longer creates it, "exists" means the operator applied {@code schema-<dialect>.sql}
     * before starting the application. Assembly, by contrast, runs when the bean is created — a
     * malformed property brings the boot down early, naming the property that is missing.
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
    public TriggerFirer mohsTriggerFirer(DataSource dataSource, HistoryStore mohsHistoryStore, WorkQueue mohsWorkQueue, JdbcDelegate delegate) {
        return new JdbcTriggerFirer(dataSource, mohsHistoryStore, mohsWorkQueue, delegate);
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
        rejectNodeLeaseShorterThanTheTickCanRenew(engineProperties.nodeLeaseTtl());
        EngineSettings settings = new EngineSettings(engineProperties.pollInterval(), engineProperties.maxPollInterval(),
                engineProperties.batchSize(), engineProperties.dispatchConcurrency(), engineProperties.claimRounds(),
                engineProperties.leaseTtl(), engineProperties.nodeLeaseTtl(), engineProperties.watchdogTimeout(),
                engineProperties.misfireThreshold(), engineProperties.idempotencyRetention());
        return new Engine(mohsWorkQueue, mohsDispatcher, mohsHistoryStore, mohsLeaseStore, mohsJobStore, mohsNodeStore,
                mohsTriggerFirer, mohsExecutionWindowRegistry, mohsRateLimitStore, mohsClock, settings,
                mohsRunnerRegistry, mohsEngineMetrics, mohsRetryPolicyRegistry);
    }

    /**
     * A boot failure rather than a WARN, for the same reason the zoneless-clock trap is one: the
     * symptom is a live node having its running work reclaimed by a peer, once an hour, on the tick
     * that prunes — the operator would read it as a phantom failover, not as a property they set.
     * See {@link #MIN_NODE_LEASE_TTL} for where the number comes from.
     *
     * <p>The floor guards the PROPERTY here, and not the value inside {@link EngineSettings}, where
     * every other node-lease rule lives: the engine's own tests drive leases of a few hundred
     * milliseconds on purpose — a test that has to watch the heartbeat cadence cannot spend twelve
     * seconds per observation — so a seconds-scale minimum belongs to what an operator configures,
     * not to what the engine accepts.
     */
    private static void rejectNodeLeaseShorterThanTheTickCanRenew(Duration nodeLeaseTtl) {
        if (nodeLeaseTtl.compareTo(MIN_NODE_LEASE_TTL) < 0) {
            throw new IllegalStateException(
                    "mohs.engine.node-lease-ttl must be at least " + MIN_NODE_LEASE_TTL.toSeconds() + "s, got "
                            + nodeLeaseTtl.toSeconds() + "s: one tick sleeps up to node-lease-ttl/3 and then spends "
                            + "up to 7s on the idempotency prune and the queue-depth count, which at "
                            + MIN_NODE_LEASE_TTL.toSeconds() + "s still leaves one second of margin for clock skew "
                            + "between nodes and for the heartbeat's own write latency. Below that the promise "
                            + "expires while this node is alive and its peers reap the work it is still running. "
                            + "Raise it to " + MIN_NODE_LEASE_TTL.toSeconds()
                            + "s or above, or drop the property and keep the 15s default.");
        }
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
    public BatchStore mohsBatchStore(DataSource dataSource, @Qualifier("mohsClock") Clock mohsClock, JdbcDelegate delegate) {
        return new JdbcBatchStore(dataSource, mohsClock, delegate);
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
