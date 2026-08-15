package io.mohs.autoconfigure;

import java.time.Clock;
import java.util.List;

import javax.sql.DataSource;

import org.springframework.beans.factory.ObjectProvider;
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

import tools.jackson.databind.json.JsonMapper;

import io.mohs.core.Mohs;
import io.mohs.core.event.ExecutionInterceptor;
import io.mohs.core.event.ExecutionListener;
import io.mohs.core.resource.ExecutionWindow;
import io.mohs.core.resource.MohsRunner;
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
        MohsProperties.Jdbc.Dialect dialect = properties.jdbc().dialect();
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

    /**
     * ADR-0008 — a condição "modo database" tem fonte única: as duas fatias
     * são mutuamente exclusivas e exaustivas sobre {@code mohs.time.mode}
     * (valor inválido nem chega aqui — falha antes, no binding do enum
     * {@link MohsProperties.Time.Mode}), e o scheduler de resync só existe
     * na fatia que o usa.
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
         * Sincroniza uma vez no boot — bloqueio deliberado: o {@link Engine}
         * não pode partir com relógio não sincronizado — e agenda o resync
         * (ver Javadoc de {@link io.mohs.engine.SyncableClock}).
         */
        @Bean(defaultCandidate = false)
        @Qualifier("mohsClock")
        Clock mohsClock(MohsProperties properties, DataSource dataSource,
                @Qualifier("mohsClockSyncScheduler") ThreadPoolTaskScheduler mohsClockSyncScheduler) {
            DatabaseClock clock = new DatabaseClock(dataSource, properties.time().skewWarnThreshold());
            clock.sync();
            mohsClockSyncScheduler.scheduleWithFixedDelay(clock::sync, properties.time().syncInterval());
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
        return MohsExecutors.ioBoundExecutor("mohs-events", properties.engine().eventConcurrency());
    }

    @Bean(defaultCandidate = false)
    @Qualifier("mohsTickScheduler")
    public ThreadPoolTaskScheduler mohsTickScheduler() {
        return MohsExecutors.scheduler("mohs-engine-tick", 1);
    }

    /** Defaults built-in, overrides e conflito de fonte: {@link MohsRunners#assemble}. */
    @Bean(destroyMethod = "close")
    public RunnerRegistry mohsRunnerRegistry(MohsProperties properties, List<MohsRunner> mohsRunnerBeans) {
        return new RunnerRegistry(MohsRunners.assemble(properties, mohsRunnerBeans));
    }

    @Bean
    public Claimer mohsClaimer(DataSource dataSource, JdbcDialect mohsJdbcDialect, @Qualifier("mohsClock") Clock mohsClock,
            ExecutionStore mohsExecutionStore, JobStore mohsJobStore, MohsProperties properties,
            ExecutionWindowRegistry mohsExecutionWindowRegistry) {
        return new JdbcClaimer(dataSource, mohsJdbcDialect, mohsClock, mohsExecutionStore, mohsJobStore,
                properties.engine().leaseTtl(), mohsExecutionWindowRegistry);
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
                properties.engine().pollInterval(), properties.engine().batchSize(),
                mohsTickScheduler, mohsRunnerRegistry);
    }

    /** {@link SmartLifecycle} — ver Javadoc de {@link MohsEngineLifecycle} sobre a adaptação e o WARN de lease × timeout. */
    @Bean
    public SmartLifecycle mohsEngineLifecycle(Engine mohsEngine, MohsProperties properties, JobStore mohsJobStore) {
        boolean autoStartup = properties.lifecycle().startMode() == MohsProperties.Lifecycle.StartMode.AUTO;
        return new MohsEngineLifecycle(mohsEngine, autoStartup, properties.lifecycle().shutdown().gracePeriod(),
                mohsJobStore, properties.engine().leaseTtl());
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
