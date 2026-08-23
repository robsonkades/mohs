package io.mohs.autoconfigure;

import java.time.Clock;
import java.time.Duration;
import java.util.List;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.jdbc.autoconfigure.DataSourceProperties;
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
import io.mohs.store.jdbc.PostgresNotifyListener;
import io.mohs.store.jdbc.JdbcTriggerFirer;
import io.mohs.store.jdbc.JdbcWorkQueue;
import io.mohs.store.jdbc.MohsFlyway;
import io.mohs.store.jdbc.PostgresPartitionManager;
import io.mohs.store.jdbc.dialect.H2JdbcDialect;
import io.mohs.store.jdbc.dialect.JdbcDialect;
import io.mohs.store.jdbc.dialect.MySqlJdbcDialect;
import io.mohs.store.jdbc.dialect.PostgresJdbcDialect;
import io.mohs.store.jdbc.dialect.SqlServerJdbcDialect;

/**
 * Liga o motor M3 ({@code io.mohs.engine}/{@code io.mohs.store.jdbc}) a um
 * {@link DataSource} Spring Boot real. Livre pra depender de internos —
 * {@code ArchitectureTest.PUBLIC_API} exclui {@code io.mohs.autoconfigure}
 * da lista de pacotes barrados de enxergar {@code io.mohs.engine}/
 * {@code io.mohs.store.jdbc}, é exatamente o papel deste pacote.
 *
 * <p>Escaneamento de {@code @MohsJob} ({@link MohsJobScanner}) e runners
 * nomeados ({@link RunnerRegistry}) são montados aqui.
 *
 * <p>O {@link ThreadPoolTaskScheduler} do resync do {@link DatabaseClock}
 * (o loop do {@link Engine} é thread própria desde a Phase 6, não bean) e o
 * {@link AsyncTaskExecutor} de eventos convivem com executores da própria
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
 *
 * <p>Nenhum bean daqui recua com {@code @ConditionalOnMissingBean} — é
 * deliberado, não omissão (ADR-0031): infraestrutura interna não é ponto
 * de extensão; a superfície do host é o vocabulário de {@code io.mohs.core}
 * coletado como beans, mais as propriedades validadas.
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
                            + "ADR-0023: the JDBC dialect is never auto-detected from the DataSource");
        }
        // ADR-0050 (tiering): H2 é Tier 3 — teste/dev apenas. O SKIP LOCKED
        // dele tem corrida real medida (~33% de double-lock, Javadoc de
        // JdbcWorkQueue); a corretude do claim vem do CAS guardado, mas ninguém
        // deve descobrir isso em produção. WARN, não erro: o demo e o dev
        // loop dependem dele de propósito.
        if (dialect == MohsProperties.Jdbc.Dialect.H2) {
            log.warn("mohs.jdbc.dialect=h2: H2 is Tier 3 — a test/dev backend, NOT supported in production (ADR-0050)");
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
        Clock mohsClock(MohsProperties properties, DataSource dataSource, @Qualifier("mohsClockSyncScheduler") ThreadPoolTaskScheduler mohsClockSyncScheduler) {
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

    /**
     * ADR-0048: as migrações do Mohs rodam na criação DESTE bean, e a ordem
     * é garantida pelo GRAFO, não pela ordem de registro: todo bean que
     * toca tabela do Mohs (stores, claimer, reaper, trigger firer) recebe
     * {@code MohsFlyway} como parâmetro — um bean do host que injete
     * {@code Mohs} e escreva no construtor força a cadeia inteira e ainda
     * assim passa por aqui primeiro (review da Phase 2: os escritores do
     * Mohs já eram tardios por construção — scanner/registrador são
     * {@code afterSingletonsInstantiated}, engine é {@code SmartLifecycle}
     * — mas o host não tinha aresta nenhuma). Bean sempre presente;
     * {@code mohs.jdbc.migrate=false} só pula o {@code migrate()}.
     * Instância e histórico próprios ({@code mohs_schema_history}) — ver
     * Javadoc de {@link MohsFlyway} sobre por que nunca o Flyway do host.
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
     * O {@code JsonMapper} cru é deliberado (ADR-0029): o formato
     * persistido de payload pertence ao Mohs, não à config web do host —
     * trocar pro {@code ObjectMapper} do contexto deixaria a config HTTP
     * do app definir um formato durável compartilhado entre nós e
     * quebraria a leitura de payloads já gravados quando ela mudar.
     */
    @Bean
    public HistoryStore mohsHistoryStore(DataSource dataSource, JdbcDialect mohsJdbcDialect, MohsFlyway mohsFlyway) {
        return new JdbcHistoryStore(dataSource, JsonMapper.builder().build(), mohsJdbcDialect);
    }

    @Bean
    public WorkQueue mohsWorkQueue(DataSource dataSource, JdbcDialect mohsJdbcDialect, BatchStore mohsBatchStore,
            @Qualifier("mohsClock") Clock mohsClock, MohsFlyway mohsFlyway) {
        return new JdbcWorkQueue(dataSource, mohsJdbcDialect, mohsBatchStore, mohsClock);
    }

    /**
     * Tier 2 do wake-up (§5.5, ADR-G) — só Postgres, e só com
     * {@code spring.datasource.url} EXPLÍCITA: a conexão do LISTEN é
     * dedicada, fora do pool, e precisa da url crua — a condição testa
     * exatamente o que o bean consome ({@code getUrl}, nunca
     * {@code determineUrl}: o fallback pra embedded derivaria uma url de
     * H2 fantasma, e a ausência viraria erro de BOOT num componente cuja
     * própria doc diz que NOTIFY nunca é requisito de correção — review
     * S6.3). App com {@code DataSource} próprio e sem a property fica sem
     * tier 2 e o poll adaptativo cobre; o relatório de conditions do Boot
     * conta o porquê.
     */
    @Bean(initMethod = "start", destroyMethod = "close")
    @ConditionalOnProperty(name = "mohs.jdbc.dialect", havingValue = "postgresql")
    @ConditionalOnProperty(name = "spring.datasource.url")
    @ConditionalOnBean(DataSourceProperties.class)
    public PostgresNotifyListener mohsNotifyListener(DataSourceProperties dataSourceProperties, Engine mohsEngine) {
        return new PostgresNotifyListener(dataSourceProperties.getUrl(),
                dataSourceProperties.getUsername(), dataSourceProperties.getPassword(),
                mohsEngine::ownsShard, mohsEngine::signalWorkScheduled);
    }

    @Bean
    public LeaseStore mohsLeaseStore(DataSource dataSource, JdbcDialect mohsJdbcDialect, BatchStore mohsBatchStore, MohsFlyway mohsFlyway) {
        return new JdbcLeaseStore(dataSource, mohsJdbcDialect, mohsBatchStore);
    }

    /** A fronteira transacional da unidade de enqueue (§7.5-1) — REQUIRED: junta-se à transação do host (ADR-0003 §4) ou abre a própria. */
    @Bean
    public StoreTransactions mohsStoreTransactions(DataSource dataSource, MohsFlyway mohsFlyway) {
        return new JdbcStoreTransactions(dataSource);
    }

    /**
     * Tier 1 apenas: as partições semanais da história são criadas AQUI, no
     * boot — antes de qualquer enqueue (o gestor depende do Flyway pelo
     * grafo, e todo escritor depende dele) — e re-garantidas a cada boot; a
     * DEFAULT das migrações é o backstop (PLAN.md S5.1). Sem bean nos
     * demais dialetos: os equivalentes funcionais não particionam
     * (ADR-0050).
     */
    @Bean
    @ConditionalOnProperty(prefix = "mohs.jdbc", name = "dialect", havingValue = "postgresql")
    public PostgresPartitionManager mohsPartitionManager(DataSource dataSource, @Qualifier("mohsClock") Clock mohsClock, MohsFlyway mohsFlyway) {
        PostgresPartitionManager manager = new PostgresPartitionManager(dataSource);
        manager.ensureWeeklyPartitions(mohsClock.instant());
        return manager;
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
     * Registro dos limites declarados DEPOIS de todos os singletons, como
     * {@link MohsJobScanner} faz com as definições: escrita em banco no
     * boot só é segura quando o schema já existe — desde a ADR-0048, quem
     * o garante é o Flyway do próprio Mohs ({@link #mohsFlyway}), criado
     * antes de qualquer singleton que toque as tabelas. A montagem, essa sim, roda na criação do bean — property
     * malformada derruba o boot cedo, com o nome da propriedade que falta.
     */
    @Bean
    public SmartInitializingSingleton mohsRateLimitRegistrar(RateLimitStore mohsRateLimitStore, MohsProperties properties,
            List<RateLimit> mohsRateLimitBeans) {
        List<RateLimit> declared = MohsRateLimits.assemble(properties, mohsRateLimitBeans);
        return () -> MohsRateLimits.register(mohsRateLimitStore, properties.registration().onConflict(), declared);
    }

    /** Sem caminho via propriedade — {@link ExecutionWindow} só existe via {@code @Bean} (ver Javadoc da classe). */
    @Bean
    public ExecutionWindowRegistry mohsExecutionWindowRegistry(List<ExecutionWindow> mohsExecutionWindowBeans) {
        return new ExecutionWindowRegistry(mohsExecutionWindowBeans);
    }

    @Bean
    public TriggerFirer mohsTriggerFirer(DataSource dataSource, HistoryStore mohsHistoryStore, WorkQueue mohsWorkQueue, MohsFlyway mohsFlyway) {
        return new JdbcTriggerFirer(dataSource, mohsHistoryStore, mohsWorkQueue);
    }

    /** Nasce vazio — {@link MohsJobScanner} povoa em {@code afterSingletonsInstantiated}, antes do {@link Engine} iniciar. */
    @Bean
    public HandlerRegistry mohsHandlerRegistry() {
        return new HandlerRegistry();
    }

    /**
     * §14.4 do redesign: métricas sempre ligadas. O host com Micrometer no
     * contexto (actuator) enxerga tudo em {@code mohs.*}; sem registry, um
     * {@link SimpleMeterRegistry} local mantém o engine idêntico — inerte
     * para o host, sem caminho condicional no código quente.
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
            ObjectProvider<CompletionBatcher> mohsCompletionBatcher
    ) {
        return new Dispatcher(mohsLeaseStore, mohsJobStore, mohsHandlerRegistry, mohsClock, interceptors, listeners,
                mohsEventExecutor, mohsEngineMetrics, mohsCompletionBatcher.getIfAvailable());
    }

    /**
     * Group commit da conclusão (ADR-0047) — N=256/T=5ms fixos por decisão
     * (§7.6: o único knob é o opt-out). {@code start}/{@code close} pelo
     * ciclo de vida do contexto: o {@code SmartLifecycle} de
     * {@link #mohsEngineLifecycle} para o engine ANTES da destruição de
     * beans, então o close drena o que os últimos handlers submeteram.
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
            EngineMetrics mohsEngineMetrics
    ) {
        MohsProperties.Engine engineProperties = properties.engine();
        EngineSettings settings = new EngineSettings(engineProperties.pollInterval(), engineProperties.maxPollInterval(),
                engineProperties.batchSize(), engineProperties.dispatchConcurrency(), engineProperties.claimRounds(),
                engineProperties.leaseTtl(), engineProperties.nodeLeaseTtl(), engineProperties.watchdogTimeout(),
                engineProperties.misfireThreshold());
        return new Engine(mohsWorkQueue, mohsDispatcher, mohsHistoryStore, mohsLeaseStore, mohsJobStore, mohsNodeStore,
                mohsTriggerFirer, mohsExecutionWindowRegistry, mohsRateLimitStore, mohsClock, settings,
                mohsRunnerRegistry, mohsEngineMetrics);
    }

    /** {@link SmartLifecycle} — ver Javadoc de {@link MohsEngineLifecycle} sobre a adaptação e o WARN de lease × timeout. */
    @Bean
    public SmartLifecycle mohsEngineLifecycle(Engine mohsEngine, MohsProperties properties, JobStore mohsJobStore) {
        boolean autoStartup = properties.lifecycle().startMode() == MohsProperties.Lifecycle.StartMode.AUTO;
        return new MohsEngineLifecycle(mohsEngine, autoStartup, properties.lifecycle().shutdown().gracePeriod(),
                mohsJobStore, properties.engine().watchdogTimeout());
    }

    @Bean
    public BatchStore mohsBatchStore(DataSource dataSource, @Qualifier("mohsClock") Clock mohsClock, MohsFlyway mohsFlyway) {
        return new JdbcBatchStore(dataSource, mohsClock);
    }

    /**
     * Entra na lista de {@code ExecutionListener} do {@link #mohsDispatcher}
     * como qualquer outro: é assim que {@code Batch.onCompletion} recebe o
     * {@code BatchCompleted} que o dispatcher publica (ADR-0043), sem
     * caminho de entrega paralelo.
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
