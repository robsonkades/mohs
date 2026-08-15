package io.mohs.autoconfigure;

import java.time.Clock;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.jspecify.annotations.Nullable;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import tools.jackson.databind.json.JsonMapper;

import io.mohs.core.Mohs;
import io.mohs.core.event.ExecutionInterceptor;
import io.mohs.core.event.ExecutionListener;
import io.mohs.core.resource.ExecutionWindow;
import io.mohs.core.resource.MohsRunner;
import io.mohs.core.resource.RunnerMode;
import io.mohs.engine.Claimer;
import io.mohs.engine.Dispatcher;
import io.mohs.engine.Engine;
import io.mohs.engine.ExecutionStore;
import io.mohs.engine.ExecutionWindowRegistry;
import io.mohs.engine.HandlerRegistry;
import io.mohs.engine.JobStore;
import io.mohs.engine.MohsExecutors;
import io.mohs.engine.MohsImpl;
import io.mohs.engine.NodeStore;
import io.mohs.engine.Reaper;
import io.mohs.engine.RunnerRegistry;
import io.mohs.jdbc.DatabaseClock;
import io.mohs.jdbc.JdbcClaimer;
import io.mohs.jdbc.JdbcExecutionStore;
import io.mohs.jdbc.JdbcJobStore;
import io.mohs.jdbc.JdbcNodeStore;
import io.mohs.jdbc.JdbcReaper;
import io.mohs.jdbc.dialect.H2JdbcDialect;
import io.mohs.jdbc.dialect.JdbcDialect;
import io.mohs.jdbc.dialect.MySqlJdbcDialect;
import io.mohs.jdbc.dialect.PostgresJdbcDialect;
import io.mohs.jdbc.dialect.SqlServerJdbcDialect;

/**
 * Liga o motor M3 ({@code io.mohs.engine}/{@code io.mohs.jdbc}) a um
 * {@link DataSource} Spring Boot real. Livre pra depender de internos —
 * {@code ArchitectureTest.PUBLIC_API} exclui {@code io.mohs.autoconfigure}
 * da lista de pacotes barrados de enxergar {@code io.mohs.engine}/
 * {@code io.mohs.jdbc}, é exatamente o papel deste pacote.
 *
 * <p>Validações de boot, wiring do REST e enforcement de rate limit ainda
 * não existem — {@link Mohs#batch} lança
 * {@link UnsupportedOperationException} (ver Javadoc de {@link MohsImpl}).
 * Escaneamento de {@code @MohsJob} ({@link MohsJobScanner}) e runners
 * nomeados ({@link RunnerRegistry}) já existem e são montados aqui.
 *
 * <p>Dois beans compartilham o tipo {@link ThreadPoolTaskScheduler} (tick
 * do {@link Engine} vs. resync do {@link DatabaseClock}), e o
 * {@link AsyncTaskExecutor} de eventos convive com executores da própria
 * aplicação hospedeira (ex.: {@code applicationTaskExecutor}) —
 * {@link Qualifier} explícito em cada ponto de injeção em vez de confiar
 * no fallback de resolução por nome do Spring (CLAUDE.md: "não usar mágica
 * onde código explícito resolve").
 *
 * <p>Todo bean de tipo genérico do framework ({@link Clock},
 * {@link ThreadPoolTaskScheduler}, {@link AsyncTaskExecutor}) é
 * {@code defaultCandidate = false}: são infraestrutura interna do motor,
 * não API compartilhada com o hospedeiro. Sem isso, a mera presença do
 * Mohs no classpath suprimia o {@code taskScheduler}/
 * {@code applicationTaskExecutor} auto-configurados do app (as conditions
 * do Boot são {@code @ConditionalOnMissingBean} por tipo, e esta
 * auto-config ordena antes das do Boot) e um segundo {@code Clock} no
 * contexto quebrava injeção não qualificada que o app já tinha —
 * degradação silenciosa do hospedeiro. Os pontos de injeção internos
 * continuam funcionando via {@link Qualifier}.
 */
@AutoConfiguration(after = DataSourceAutoConfiguration.class)
@ConditionalOnProperty(prefix = "mohs", name = "enabled", matchIfMissing = true)
@EnableConfigurationProperties(MohsProperties.class)
public class MohsAutoConfiguration {

    @Bean
    public JdbcDialect mohsJdbcDialect(MohsProperties properties) {
        MohsProperties.Jdbc.Dialect dialect = properties.getJdbc().getDialect();
        if (dialect == null) {
            throw new IllegalStateException(
                    "mohs.jdbc.dialect must be set (h2, postgresql, mysql or sqlserver) — "
                            + "ADR-0023: the JDBC dialect is never auto-detected from the DataSource");
        }
        return switch (dialect) {
            case H2 -> new H2JdbcDialect();
            case POSTGRESQL -> new PostgresJdbcDialect();
            case MYSQL -> new MySqlJdbcDialect();
            case SQLSERVER -> new SqlServerJdbcDialect();
        };
    }

    @Bean(defaultCandidate = false)
    @Qualifier("mohsClockSyncScheduler")
    @ConditionalOnProperty(prefix = "mohs.time", name = "mode", havingValue = "database")
    public ThreadPoolTaskScheduler mohsClockSyncScheduler() {
        return MohsExecutors.scheduler("mohs-clock-sync", 1);
    }

    /** {@code database}: sincroniza uma vez no boot e agenda resync periódico (ver Javadoc de {@link io.mohs.engine.SyncableClock}). */
    @Bean(defaultCandidate = false)
    @Qualifier("mohsClock")
    public Clock mohsClock(MohsProperties properties, DataSource dataSource, @Qualifier("mohsClockSyncScheduler") ObjectProvider<ThreadPoolTaskScheduler> mohsClockSyncScheduler) {
        if (properties.getTime().getMode() != MohsProperties.Time.Mode.DATABASE) {
            return Clock.systemUTC();
        }
        DatabaseClock clock = new DatabaseClock(dataSource, properties.getTime().getSkewWarnThreshold());
        clock.sync();
        mohsClockSyncScheduler.getObject().scheduleWithFixedDelay(clock::sync, properties.getTime().getSyncInterval());
        return clock;
    }

    @Bean
    public JobStore mohsJobStore(DataSource dataSource, @Qualifier("mohsClock") Clock mohsClock) {
        return new JdbcJobStore(dataSource, mohsClock);
    }

    @Bean
    public ExecutionStore mohsExecutionStore(DataSource dataSource, @Qualifier("mohsClock") Clock mohsClock, JdbcDialect mohsJdbcDialect) {
        return new JdbcExecutionStore(dataSource, mohsClock, JsonMapper.builder().build(), mohsJdbcDialect);
    }

    @Bean
    public NodeStore mohsNodeStore(DataSource dataSource) {
        return new JdbcNodeStore(dataSource);
    }

    @Bean(defaultCandidate = false)
    @Qualifier("mohsEventExecutor")
    public AsyncTaskExecutor mohsEventExecutor(MohsProperties properties) {
        return MohsExecutors.ioBoundExecutor("mohs-events", properties.getEngine().getEventConcurrency());
    }

    @Bean(defaultCandidate = false)
    @Qualifier("mohsTickScheduler")
    public ThreadPoolTaskScheduler mohsTickScheduler() {
        return MohsExecutors.scheduler("mohs-engine-tick", 1);
    }

    /**
     * {@code io}/{@code cpu} built-in sempre presentes (defaults do
     * documento mestre — {@code io} reaproveita
     * {@code mohs.engine.dispatch-concurrency}, mesmo papel que tinha
     * quando ainda era o único executor de dispatch fixo). Nome duplicado
     * entre {@code mohs.runners.*} e {@code @Bean MohsRunner} é erro de
     * boot — mesma filosofia de "conflito de identidade falha sempre" já
     * usada pro {@code annotation × programmatic} do {@link MohsJobScanner}.
     */
    @Bean(destroyMethod = "close")
    public RunnerRegistry mohsRunnerRegistry(MohsProperties properties, List<MohsRunner> mohsRunnerBeans) {
        Map<String, MohsRunner> byName = new LinkedHashMap<>();
        Map<String, String> sourceOf = new LinkedHashMap<>();

        byName.put(RunnerRegistry.DEFAULT_RUNNER, MohsRunner.io(RunnerRegistry.DEFAULT_RUNNER).maxConcurrent(properties.getEngine().getDispatchConcurrency()).build());
        sourceOf.put(RunnerRegistry.DEFAULT_RUNNER, "built-in");
        byName.put("cpu", MohsRunner.cpu("cpu").build());
        sourceOf.put("cpu", "built-in");

        properties.getRunners().forEach((name, spec) -> {
            requireNoRunnerConflict(name, "mohs.runners." + name, sourceOf);
            byName.put(name, toMohsRunner(name, spec));
            sourceOf.put(name, "mohs.runners." + name);
        });
        for (MohsRunner beanRunner : mohsRunnerBeans) {
            requireNoRunnerConflict(beanRunner.name(), "@Bean MohsRunner " + beanRunner.name(), sourceOf);
            byName.put(beanRunner.name(), beanRunner);
            sourceOf.put(beanRunner.name(), "@Bean MohsRunner " + beanRunner.name());
        }

        return new RunnerRegistry(List.copyOf(byName.values()));
    }

    private static void requireNoRunnerConflict(String name, String newSource, Map<String, String> sourceOf) {
        String existing = sourceOf.get(name);
        if (existing != null && !existing.equals("built-in")) {
            throw new IllegalStateException("runner '" + name + "' declared more than once: " + existing + " and " + newSource);
        }
    }

    /**
     * Campo do modo errado é erro de boot, nunca descarte silencioso —
     * mesma postura do compact constructor de {@link MohsRunner}, que lança
     * pra campo do modo errado (e mesma filosofia de "conflito de identidade
     * falha sempre" de {@link #requireNoRunnerConflict}): {@code core-size=2}
     * com {@code mode} esquecido no default {@code io} viraria um runner de
     * 64 virtual threads pra trabalho CPU-bound, sem aviso nenhum. A
     * validação do próprio builder ganha o contexto que só a propriedade tem
     * ("maxSize must be >= coreSize" sozinho não diz qual runner nem qual
     * propriedade — e {@code core-size} default depende dos núcleos da
     * máquina, então o boot falharia só em produção).
     */
    private static MohsRunner toMohsRunner(String name, MohsProperties.Runner spec) {
        String prefix = "mohs.runners." + name;
        try {
            return switch (spec.getMode()) {
                case IO -> {
                    requireUnset(prefix, spec.getMode(), "core-size", spec.getCoreSize());
                    requireUnset(prefix, spec.getMode(), "max-size", spec.getMaxSize());
                    requireUnset(prefix, spec.getMode(), "queue-capacity", spec.getQueueCapacity());
                    requireUnset(prefix, spec.getMode(), "keep-alive", spec.getKeepAlive());
                    MohsRunner.IoBuilder builder = MohsRunner.io(name);
                    if (spec.getMax() != null) {
                        builder.maxConcurrent(spec.getMax());
                    }
                    yield builder.build();
                }
                case CPU -> {
                    requireUnset(prefix, spec.getMode(), "max", spec.getMax());
                    MohsRunner.CpuBuilder builder = MohsRunner.cpu(name);
                    if (spec.getCoreSize() != null) {
                        builder.coreSize(spec.getCoreSize());
                    }
                    if (spec.getMaxSize() != null) {
                        builder.maxSize(spec.getMaxSize());
                    }
                    if (spec.getQueueCapacity() != null) {
                        builder.queueCapacity(spec.getQueueCapacity());
                    }
                    if (spec.getKeepAlive() != null) {
                        builder.keepAlive(spec.getKeepAlive());
                    }
                    yield builder.build();
                }
            };
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException("invalid runner declared at " + prefix + ".*: " + e.getMessage(), e);
        }
    }

    private static void requireUnset(String prefix, RunnerMode mode, String property, @Nullable Object value) {
        if (value != null) {
            throw new IllegalStateException("invalid runner declared at " + prefix + ".*: " + property
                    + " does not apply to mode=" + mode + " — change " + prefix + ".mode or remove " + prefix + "." + property);
        }
    }

    @Bean
    public Claimer mohsClaimer(DataSource dataSource, JdbcDialect mohsJdbcDialect, @Qualifier("mohsClock") Clock mohsClock,
            ExecutionStore mohsExecutionStore, JobStore mohsJobStore, MohsProperties properties,
            ExecutionWindowRegistry mohsExecutionWindowRegistry) {
        return new JdbcClaimer(dataSource, mohsJdbcDialect, mohsClock, mohsExecutionStore, mohsJobStore,
                properties.getEngine().getLeaseTtl(), mohsExecutionWindowRegistry);
    }

    /** Sem caminho via propriedade — {@link ExecutionWindow} só existe via {@code @Bean} (ver Javadoc da classe). */
    @Bean
    public ExecutionWindowRegistry mohsExecutionWindowRegistry(List<ExecutionWindow> mohsExecutionWindowBeans) {
        return new ExecutionWindowRegistry(mohsExecutionWindowBeans);
    }

    @Bean
    public Reaper mohsReaper(DataSource dataSource, @Qualifier("mohsClock") Clock mohsClock, ExecutionStore mohsExecutionStore, JobStore mohsJobStore) {
        return new JdbcReaper(dataSource, mohsClock, mohsExecutionStore, mohsJobStore);
    }

    /** Nasce vazio — {@link MohsJobScanner} povoa em {@code afterSingletonsInstantiated}, antes do {@link Engine} iniciar. */
    @Bean
    public HandlerRegistry mohsHandlerRegistry() {
        return new HandlerRegistry();
    }

    @Bean
    public Dispatcher mohsDispatcher(
            ExecutionStore mohsExecutionStore,
            JobStore mohsJobStore,
            HandlerRegistry mohsHandlerRegistry,
            @Qualifier("mohsClock") Clock mohsClock,
            List<ExecutionInterceptor> interceptors,
            List<ExecutionListener> listeners,
            @Qualifier("mohsEventExecutor") AsyncTaskExecutor mohsEventExecutor
    ) {
        return new Dispatcher(mohsExecutionStore, mohsJobStore, mohsHandlerRegistry, mohsClock, interceptors, listeners, mohsEventExecutor);
    }

    @Bean
    public Engine mohsEngine(
            Claimer mohsClaimer,
            Dispatcher mohsDispatcher,
            ExecutionStore mohsExecutionStore,
            JobStore mohsJobStore,
            NodeStore mohsNodeStore,
            Reaper mohsReaper,
            @Qualifier("mohsClock") Clock mohsClock,
            MohsProperties properties,
            @Qualifier("mohsTickScheduler") ThreadPoolTaskScheduler mohsTickScheduler,
            RunnerRegistry mohsRunnerRegistry
    ) {
        return new Engine(mohsClaimer, mohsDispatcher, mohsExecutionStore, mohsJobStore, mohsNodeStore, mohsReaper, mohsClock,
                properties.getEngine().getPollInterval(), properties.getEngine().getBatchSize(),
                mohsTickScheduler, mohsRunnerRegistry);
    }

    /** {@link SmartLifecycle} — ver Javadoc de {@link MohsEngineLifecycle} sobre a adaptação e o WARN de lease × timeout. */
    @Bean
    public SmartLifecycle mohsEngineLifecycle(Engine mohsEngine, MohsProperties properties, JobStore mohsJobStore) {
        boolean autoStartup = properties.getLifecycle().getStartMode() == MohsProperties.Lifecycle.StartMode.AUTO;
        return new MohsEngineLifecycle(mohsEngine, autoStartup, properties.getLifecycle().getShutdown().getGracePeriod(),
                mohsJobStore, properties.getEngine().getLeaseTtl());
    }

    @Bean
    public Mohs mohs(JobStore mohsJobStore, ExecutionStore mohsExecutionStore, HandlerRegistry mohsHandlerRegistry,
            @Qualifier("mohsClock") Clock mohsClock, Engine mohsEngine) {
        return new MohsImpl(mohsJobStore, mohsExecutionStore, mohsHandlerRegistry, mohsClock, mohsEngine);
    }

    /**
     * {@code static}: {@code BeanPostProcessor} via {@code @Bean} não-estático
     * arrisca inicializar esta classe de configuração cedo demais (aviso
     * conhecido do próprio Spring) — {@code static} evita, sem abrir mão de
     * parâmetros autowired normais. {@code ObjectProvider} nos três
     * parâmetros pelo mesmo motivo, do lado de {@link MohsJobScanner}: ver
     * o Javadoc de classe dela.
     */
    @Bean
    public static MohsJobScanner mohsJobScanner(
            ObjectProvider<HandlerRegistry> mohsHandlerRegistry,
            ObjectProvider<JobStore> mohsJobStore,
            ObjectProvider<MohsProperties> properties
    ) {
        return new MohsJobScanner(mohsHandlerRegistry, mohsJobStore, properties);
    }
}
