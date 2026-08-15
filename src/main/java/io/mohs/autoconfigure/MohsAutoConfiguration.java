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
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import tools.jackson.databind.json.JsonMapper;

import io.mohs.core.Mohs;
import io.mohs.core.event.ExecutionInterceptor;
import io.mohs.core.event.ExecutionListener;
import io.mohs.engine.Claimer;
import io.mohs.engine.Dispatcher;
import io.mohs.engine.Engine;
import io.mohs.engine.ExecutionStore;
import io.mohs.engine.HandlerRegistry;
import io.mohs.engine.JobStore;
import io.mohs.engine.MohsExecutors;
import io.mohs.engine.MohsImpl;
import io.mohs.engine.NodeStore;
import io.mohs.engine.Reaper;
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
 * <p>Escaneamento de {@code @MohsJob}, validações de boot, wiring do REST e
 * enforcement de runner/rate limit ainda não existem — {@link HandlerRegistry}
 * nasce vazio nesta rodada, {@link Mohs#batch} lança
 * {@link UnsupportedOperationException} (ver Javadoc de {@link MohsImpl}).
 *
 * <p>Dois pares de beans compartilham tipo ({@link ThreadPoolTaskScheduler}
 * pro tick do {@link Engine} vs. resync do {@link DatabaseClock};
 * {@link AsyncTaskExecutor} pro dispatch vs. eventos) — {@link Qualifier}
 * explícito em cada ponto de injeção em vez de confiar no fallback de
 * resolução por nome do Spring (CLAUDE.md: "não usar mágica onde código
 * explícito resolve").
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

    @Bean
    @Qualifier("mohsClockSyncScheduler")
    @ConditionalOnProperty(prefix = "mohs.time", name = "mode", havingValue = "database")
    public ThreadPoolTaskScheduler mohsClockSyncScheduler() {
        return MohsExecutors.scheduler("mohs-clock-sync", 1);
    }

    /** {@code database}: sincroniza uma vez no boot e agenda resync periódico (ver Javadoc de {@link io.mohs.engine.SyncableClock}). */
    @Bean
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
    public JobStore mohsJobStore(DataSource dataSource, Clock mohsClock) {
        return new JdbcJobStore(dataSource, mohsClock);
    }

    @Bean
    public ExecutionStore mohsExecutionStore(DataSource dataSource, Clock mohsClock) {
        return new JdbcExecutionStore(dataSource, mohsClock, JsonMapper.builder().build());
    }

    @Bean
    public NodeStore mohsNodeStore(DataSource dataSource) {
        return new JdbcNodeStore(dataSource);
    }

    @Bean
    @Qualifier("mohsDispatchExecutor")
    public AsyncTaskExecutor mohsDispatchExecutor(MohsProperties properties) {
        return MohsExecutors.ioBoundExecutor("mohs-dispatch", properties.getEngine().getDispatchConcurrency());
    }

    @Bean
    @Qualifier("mohsEventExecutor")
    public AsyncTaskExecutor mohsEventExecutor(MohsProperties properties) {
        return MohsExecutors.ioBoundExecutor("mohs-events", properties.getEngine().getEventConcurrency());
    }

    @Bean
    @Qualifier("mohsTickScheduler")
    public ThreadPoolTaskScheduler mohsTickScheduler() {
        return MohsExecutors.scheduler("mohs-engine-tick", 1);
    }

    @Bean
    public Claimer mohsClaimer(DataSource dataSource, JdbcDialect mohsJdbcDialect, Clock mohsClock,
            ExecutionStore mohsExecutionStore, JobStore mohsJobStore, MohsProperties properties) {
        return new JdbcClaimer(dataSource, mohsJdbcDialect, mohsClock, mohsExecutionStore, mohsJobStore,
                properties.getEngine().getLeaseTtl());
    }

    @Bean
    public Reaper mohsReaper(DataSource dataSource, Clock mohsClock, ExecutionStore mohsExecutionStore, JobStore mohsJobStore) {
        return new JdbcReaper(dataSource, mohsClock, mohsExecutionStore, mohsJobStore);
    }

    /** Vazio nesta rodada — escaneamento de {@code @MohsJob} (fora de escopo) é quem povoa. */
    @Bean
    public HandlerRegistry mohsHandlerRegistry() {
        return new HandlerRegistry();
    }

    @Bean
    public Dispatcher mohsDispatcher(ExecutionStore mohsExecutionStore, JobStore mohsJobStore, HandlerRegistry mohsHandlerRegistry,
            Clock mohsClock, List<ExecutionInterceptor> interceptors, List<ExecutionListener> listeners,
            @Qualifier("mohsEventExecutor") AsyncTaskExecutor mohsEventExecutor) {
        return new Dispatcher(mohsExecutionStore, mohsJobStore, mohsHandlerRegistry, mohsClock, interceptors, listeners, mohsEventExecutor);
    }

    @Bean
    public Engine mohsEngine(Claimer mohsClaimer, Dispatcher mohsDispatcher, ExecutionStore mohsExecutionStore, NodeStore mohsNodeStore,
            Reaper mohsReaper, Clock mohsClock, MohsProperties properties,
            @Qualifier("mohsTickScheduler") ThreadPoolTaskScheduler mohsTickScheduler,
            @Qualifier("mohsDispatchExecutor") AsyncTaskExecutor mohsDispatchExecutor) {
        return new Engine(mohsClaimer, mohsDispatcher, mohsExecutionStore, mohsNodeStore, mohsReaper, mohsClock,
                properties.getEngine().getPollInterval(), properties.getEngine().getBatchSize(),
                mohsTickScheduler, mohsDispatchExecutor);
    }

    /** {@link SmartLifecycle} — ver Javadoc de {@link MohsEngineLifecycle} sobre a adaptação. */
    @Bean
    public SmartLifecycle mohsEngineLifecycle(Engine mohsEngine, MohsProperties properties) {
        boolean autoStartup = properties.getLifecycle().getStartMode() == MohsProperties.Lifecycle.StartMode.AUTO;
        return new MohsEngineLifecycle(mohsEngine, autoStartup, properties.getLifecycle().getShutdown().getGracePeriod());
    }

    @Bean
    public Mohs mohs(JobStore mohsJobStore, ExecutionStore mohsExecutionStore, Clock mohsClock, Engine mohsEngine) {
        return new MohsImpl(mohsJobStore, mohsExecutionStore, mohsClock, mohsEngine);
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
    public static MohsJobScanner mohsJobScanner(ObjectProvider<HandlerRegistry> mohsHandlerRegistry,
            ObjectProvider<JobStore> mohsJobStore, ObjectProvider<MohsProperties> properties) {
        return new MohsJobScanner(mohsHandlerRegistry, mohsJobStore, properties);
    }
}
