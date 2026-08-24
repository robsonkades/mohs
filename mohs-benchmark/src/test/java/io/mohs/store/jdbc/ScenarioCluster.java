package io.mohs.store.jdbc;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

import javax.sql.DataSource;

import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.jdbc.core.JdbcTemplate;

import tools.jackson.databind.json.JsonMapper;

import io.github.robsonkades.uuidv7.UUIDv7;
import io.mohs.store.jdbc.dialect.PostgresJdbcDialect;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.mohs.core.definition.JobDefinition;
import io.mohs.core.definition.PolicySpec;
import io.mohs.core.event.ExecutionListener;
import io.mohs.core.execution.ExecutionId;
import io.mohs.core.job.JobKey;
import io.mohs.core.resource.MohsRunner;
import io.mohs.engine.Dispatcher;
import io.mohs.engine.Engine;
import io.mohs.engine.EngineMetrics;
import io.mohs.engine.EngineSettings;
import io.mohs.engine.ExecutionWindowRegistry;
import io.mohs.engine.HandlerRegistry;
import io.mohs.engine.HistoryStore;
import io.mohs.engine.JobHandler;
import io.mohs.engine.BatchCompletionCallbacks;
import io.mohs.engine.CompletionBatcher;
import io.mohs.engine.MohsExecutors;
import io.mohs.engine.MohsImpl;
import io.mohs.engine.RunnerRegistry;
import io.mohs.engine.Shards;
import io.mohs.engine.WorkQueue;

/**
 * Um cluster de N nós dentro de UMA JVM, contra o Postgres real do
 * container: cada {@link Engine} tem seu próprio {@code nodeId}, epoch,
 * lease e loop de tick — do ponto de vista do banco são N nós, e é o banco
 * que arbitra claim, posse e sharding. É a fidelidade que os cenários de
 * corretude precisam, e a que os scripts de bancada
 * ({@code chaos-recovery.ps1}) não dão de graça: aqui o teste observa o
 * que cada handler viu, em memória, sem inferir do log.
 *
 * <p>A fiação é a de {@code MohsAutoConfiguration} — group commit ligado
 * nos mesmos 256/5ms, executor de eventos no mesmo teto de 16, ordem de
 * shutdown igual (engine para, batcher drena depois). Isso não é zelo: um
 * veredito de perda de trabalho tirado numa fiação que ninguém roda em
 * produção não vale como evidência de release.
 *
 * <p>As DUAS divergências que restam, declaradas:
 * <ul>
 *   <li><b>Morte de processo</b> (kill −9, freeze) não é expressável — um
 *       nó aqui morre com {@code stop()}, não com o carrier arrancado
 *       debaixo dele. Fica com {@code chaos-recovery.ps1}, que existe
 *       justamente por isso.</li>
 *   <li><b>Sem pool de conexões</b>: {@code PostgresTestSupport} entrega
 *       um {@code PGSimpleDataSource}, então cada statement paga TCP +
 *       auth. Toda latência e vazão que estes cenários imprimem é de
 *       diagnóstico, NUNCA número de release — produção usa HikariCP com
 *       {@code maximumPoolSize} 100+, e é o BASELINE.md que fala de
 *       desempenho.</li>
 * </ul>
 */
final class ScenarioCluster implements AutoCloseable {

    /** Um único offer de 100k estouraria o limite de parâmetros do driver. */
    private static final int OFFER_BATCH_SIZE = 1_000;

    /**
     * A razão entre dispatch e publicação de eventos que os defaults de
     * produção codificam (64 de dispatch para 16 de eventos). Segurar o
     * LITERAL 16 com um dispatch maior seria rodar a fiação que
     * PERFORMANCE.md desaconselha — e {@code ExecutionEventPublisher}
     * DESCARTA o evento quando satura (entrega é best-effort por contrato),
     * então a bancada ficaria vermelha por tuning dela, não por defeito do
     * produto.
     */
    private static final int DISPATCH_TO_EVENT_RATIO = 4;

    /**
     * Um nó: o engine, o registro de handlers que só ele enxerga (é o que
     * faz rolling update com handler ausente ser expressável) e os recursos
     * que o nó é dono e precisa devolver no {@code close}.
     */
    record Node(Engine engine, HandlerRegistry handlers, RunnerRegistry runners, CompletionBatcher batcher,
            SimpleAsyncTaskExecutor events, List<ExecutionListener> listeners) {
    }

    private final DataSource dataSource;
    private final Clock clock;
    private final JdbcTemplate jdbcTemplate;
    private final JdbcJobStore jobStore;
    private final JdbcHistoryStore historyStore;
    private final JdbcWorkQueue workQueue;
    private final JdbcLeaseStore leaseStore;
    private final JdbcRateLimitStore rateLimitStore;
    private final JdbcNodeStore nodeStore;
    private final JdbcBatchStore batchStore;
    private final List<Node> nodes = new ArrayList<>();

    ScenarioCluster(DataSource dataSource, Clock clock) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.jdbcTemplate = new JdbcTemplate(dataSource);
        PostgresJdbcDialect dialect = new PostgresJdbcDialect();
        this.batchStore = new JdbcBatchStore(dataSource, clock);
        this.jobStore = new JdbcJobStore(dataSource, clock);
        this.historyStore = new JdbcHistoryStore(dataSource, JsonMapper.builder().build(), dialect);
        this.workQueue = new JdbcWorkQueue(dataSource, dialect, batchStore);
        this.leaseStore = new JdbcLeaseStore(dataSource, dialect, batchStore);
        this.rateLimitStore = new JdbcRateLimitStore(dataSource, clock);
        this.nodeStore = new JdbcNodeStore(dataSource);
    }

    JdbcTemplate jdbc() {
        return jdbcTemplate;
    }

    JdbcJobStore jobs() {
        return jobStore;
    }

    JdbcRateLimitStore rateLimits() {
        return rateLimitStore;
    }

    JdbcHistoryStore history() {
        return historyStore;
    }

    List<Node> nodes() {
        return List.copyOf(nodes);
    }

    /** Registra a definição de um job on-demand; {@code policy} recebe a spec para apontar runner, limite ou cap. */
    void defineJob(String jobKey, Consumer<PolicySpec> policy) {
        jobStore.upsert(JobDefinition.of(jobKey, ScenarioCluster.class, spec -> policy.accept(spec.onDemand())));
    }

    /**
     * Um job RECORRENTE — o que faz a materialização de trigger (§5.2) entrar
     * no cenário, e com ela a corrida entre nós pela mesma ocorrência.
     * {@code retries(0)} declarado pela mesma disciplina dos demais: o
     * orçamento não é variável deste experimento (o retry reencarna a MESMA
     * linha de {@code mohs_execution}, então nem mudaria as contagens), e
     * declarar impede que a próxima revisão de default mexa no que a bancada
     * mede sem ninguém decidir.
     */
    void defineRecurring(String jobKey, Duration every) {
        jobStore.upsert(JobDefinition.of(jobKey, ScenarioCluster.class, spec -> spec.every(every).retries(0)));
    }

    /**
     * Um nó novo, ainda parado. O {@code handlers} nasce vazio: quem chama
     * registra só o que ESTE nó sabe fazer — dois nós com registros
     * diferentes é literalmente o rolling update do S9.
     */
    Node addNode(EngineSettings settings, List<ExecutionListener> listeners) {
        HandlerRegistry handlers = new HandlerRegistry();
        EngineMetrics metrics = new EngineMetrics(new SimpleMeterRegistry());
        RunnerRegistry runners = new RunnerRegistry(
                List.of(MohsRunner.io(RunnerRegistry.DEFAULT_RUNNER).maxConcurrent(settings.dispatchConcurrency()).build()));
        // group commit LIGADO, com os mesmos N/T de MohsAutoConfiguration: sem
        // ele a conclusão vira síncrona, `Dispatcher#completionInTransit`
        // devolve false constante (uma das TRÊS guardas de
        // `Engine#reconcileOwnStrayLeases`) e a janela "conclusão commitada ×
        // lease liberada" — a do incidente do S5.5 — deixa de existir. Um
        // veredito de perda no shutdown tirado sem isto seria sobre uma
        // fiação que nenhum cliente roda.
        CompletionBatcher batcher = new CompletionBatcher(leaseStore, jobStore, 256, Duration.ofMillis(5));
        batcher.start();
        // eventos escalam COM o dispatch (PERFORMANCE.md: "sob vazão alta, 16 vira
        // fila; suba junto com o dispatch") — o default de produção é a razão
        // 4:1 de 64/16, não o literal 16
        SimpleAsyncTaskExecutor events = MohsExecutors.ioBoundExecutor("mohs-events-scenario",
                Math.max(1, settings.dispatchConcurrency() / DISPATCH_TO_EVENT_RATIO));
        Dispatcher dispatcher = new Dispatcher(leaseStore, jobStore, handlers, clock, List.of(), listeners,
                events, metrics, batcher);
        Engine engine = new Engine(workQueue, dispatcher, historyStore, leaseStore, jobStore, nodeStore,
                new JdbcTriggerFirer(dataSource, historyStore, workQueue), new ExecutionWindowRegistry(List.of()),
                rateLimitStore, clock, settings, runners, metrics);
        Node node = new Node(engine, handlers, runners, batcher, events, List.copyOf(listeners));
        nodes.add(node);
        return node;
    }

    /**
     * A fachada pública ligada a ESTE nó — o caminho de escrita que uma
     * aplicação real usa ({@code Mohs.batch}, {@code Mohs.schedule}), com o
     * hand-off local apontando para o loop dele. Cenário que semeia por
     * {@link #seedReady} mede o motor; este mede o motor MAIS o caminho de
     * entrada, que é onde o lote nasce.
     */
    MohsImpl facadeFor(Node node, BatchCompletionCallbacks callbacks) {
        // BatchCompletionCallbacks É um ExecutionListener: fora da lista de
        // listeners do nó ele nunca é notificado, e um onCompletion que
        // silenciosamente nunca dispara faria a bancada reportar um defeito
        // que não existe. Falhar aqui é mais barato que a investigação.
        if (!node.listeners().contains(callbacks)) {
            throw new IllegalArgumentException("callbacks must have been passed to addNode(...) as a listener — "
                    + "BatchCompletionCallbacks is an ExecutionListener, and outside the node's listener list "
                    + "onCompletion never fires");
        }
        return new MohsImpl(jobStore, workQueue, historyStore, leaseStore, new JdbcStoreTransactions(dataSource),
                nodeStore, rateLimitStore, node.handlers(), clock, node.engine(), batchStore,
                callbacks, node.runners(), node.engine()::signalWorkScheduled);
    }

    /** Registra o mesmo handler em todos os nós já criados. */
    void registerEverywhere(String jobKey, JobHandler handler) {
        nodes.forEach(node -> node.handlers().register(JobKey.of(jobKey), handler));
    }

    void startAll() {
        nodes.forEach(node -> node.engine().start());
    }

    /**
     * Semeia {@code count} execuções prontas do job — a unidade de enqueue
     * do §7.5-1 (linha de história + entrada na fila), com o shard DERIVADO
     * do id como todo escritor real faz: semear tudo no shard 0 mediria um
     * cluster que não existe.
     */
    List<ExecutionId> seedReady(String jobKey, int count, int priority) {
        JobKey key = JobKey.of(jobKey);
        Instant now = clock.instant();
        List<ExecutionId> ids = new ArrayList<>(count);
        List<HistoryStore.NewExecution> rows = new ArrayList<>(count);
        List<WorkQueue.ReadyEntry> entries = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            ExecutionId id = ExecutionId.of(UUIDv7.randomUUID().toString());
            int shard = Shards.of(id);
            ids.add(id);
            rows.add(new HistoryStore.NewExecution(id, key, shard, priority, now, now, "scenario", null, null, ""));
            entries.add(new WorkQueue.ReadyEntry(id, key, shard, priority, 1, now));
        }
        for (int from = 0; from < count; from += OFFER_BATCH_SIZE) {
            int to = Math.min(from + OFFER_BATCH_SIZE, count);
            historyStore.record(rows.subList(from, to));
            workQueue.offer(entries.subList(from, to));
        }
        return ids;
    }

    /** Espera até a condição valer ou o teto estourar; devolve se valeu (o cenário decide se isso é falha). */
    static boolean awaitUntil(Duration timeout, BooleanSupplier condition) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return true;
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("scenario interrupted while waiting", e);
            }
        }
        return condition.getAsBoolean();
    }

    /** A fila e a posse vazias ao mesmo tempo: o critério de "drenou" de todo cenário de backlog. */
    boolean isDrained() {
        return countReady() == 0 && countLease() == 0;
    }

    int countReady() {
        return count("SELECT count(*) FROM mohs_ready");
    }

    int countLease() {
        return count("SELECT count(*) FROM mohs_lease");
    }

    int countAttempts() {
        return count("SELECT count(*) FROM mohs_attempt");
    }

    int countTerminal(String state) {
        return count("SELECT count(*) FROM mohs_execution WHERE state = ?", state);
    }

    int countExecutionsOf(String jobKey) {
        return count("SELECT count(*) FROM mohs_execution WHERE job_key = ?", jobKey);
    }

    /**
     * Como o motor classificou cada tentativa que falhou — é onde
     * {@code NO_HANDLER} e a reclamação de posse aparecem com nome, em vez
     * de virarem um número de FAILED sem explicação.
     */
    Map<String, Integer> failureKinds() {
        // por tipo E mensagem: o motor usa IllegalStateException para TRÊS
        // causas distintas — handler ausente, cancelamento por shutdown e
        // lease de nó morto. Agrupar só por tipo colapsa as três e esvazia
        // qualquer asserção de atribuição de causa.
        return jdbcTemplate.query("""
                SELECT error_type || ': ' || left(error, 60) AS kind, count(*) AS total
                  FROM mohs_attempt
                 WHERE error_type IS NOT NULL
                 GROUP BY error_type, left(error, 60)
                 ORDER BY count(*) DESC
                """, rs -> {
            Map<String, Integer> kinds = new LinkedHashMap<>();
            while (rs.next()) {
                kinds.put(rs.getString("kind"), rs.getInt("total"));
            }
            return kinds;
        });
    }

    private int count(String sql, Object... args) {
        Integer value = jdbcTemplate.queryForObject(sql, Integer.class, args);
        return value == null ? 0 : value;
    }

    /**
     * Ordem de produção ({@code MohsAutoConfiguration}): o engine para
     * primeiro, e só então o batcher drena o que os últimos handlers
     * submeteram — fechar o batcher antes descartaria conclusões que ainda
     * estavam em trânsito.
     */
    @Override
    public void close() {
        nodes.forEach(node -> {
            try {
                node.engine().stop(Duration.ofSeconds(10));
            } catch (IllegalStateException _) {
                // já parado — cenário que derruba um nó de propósito passa por aqui
            }
        });
        nodes.forEach(node -> {
            node.batcher().close();
            node.events().close();
            node.runners().close();
        });
        nodes.clear();
    }
}
