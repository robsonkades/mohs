package io.mohs.store.jdbc;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import tools.jackson.databind.json.JsonMapper;

import io.mohs.core.definition.JobDefinition;
import io.mohs.core.event.ExecutionListener;
import io.mohs.core.event.Succeeded;
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
import io.mohs.engine.MohsExecutors;
import io.mohs.engine.RunnerRegistry;
import io.mohs.engine.Shards;
import io.mohs.engine.WorkQueue;
import io.mohs.store.jdbc.dialect.PostgresJdbcDialect;
import io.mohs.test.MutableClock;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * O gate de latência do S6.3, fim a fim contra Postgres real: um enqueue
 * de OUTRA conexão (outro "nó") com o engine parado no teto do backoff
 * despacha pelo caminho do NOTIFY — bem antes do poll. Sem o tier 2, o
 * claim seguinte só viria no próximo tick (~2s); o await de 1s abaixo é
 * impossível pelo poll sozinho.
 */
class EngineNotifyPostgresTest {

    record Handler() {
    }

    private static final Instant NOW = Instant.parse("2026-08-15T12:00:00Z");

    private DataSource dataSource;
    private MutableClock clock;
    private Engine engine;
    private PostgresNotifyListener listener;
    private final HandlerRegistry handlerRegistry = new HandlerRegistry();
    private final CountDownLatch succeeded = new CountDownLatch(1);

    @BeforeEach
    void setUp() {
        dataSource = PostgresTestSupport.freshSchema();
        clock = new MutableClock(NOW, ZoneId.of("UTC"));
        new PostgresPartitionManager(dataSource).ensureWeeklyPartitions(NOW);

        JdbcBatchStore batchStore = new JdbcBatchStore(dataSource, clock);
        JdbcJobStore jobStore = new JdbcJobStore(dataSource, clock);
        JdbcHistoryStore historyStore = new JdbcHistoryStore(dataSource, JsonMapper.builder().build(), new PostgresJdbcDialect());
        JdbcWorkQueue workQueue = new JdbcWorkQueue(dataSource, new PostgresJdbcDialect(), batchStore, clock);
        JdbcLeaseStore leaseStore = new JdbcLeaseStore(dataSource, new PostgresJdbcDialect(), batchStore);
        JdbcNodeStore nodeStore = new JdbcNodeStore(dataSource);
        JdbcRateLimitStore rateLimitStore = new JdbcRateLimitStore(dataSource, clock);

        jobStore.upsert(JobDefinition.of("welcome-email", Handler.class, spec -> spec.onDemand()));
        handlerRegistry.register(JobKey.of("welcome-email"), (payload, ctx) -> {
        });
        ExecutionListener listenerBean = event -> {
            if (event instanceof Succeeded) {
                succeeded.countDown();
            }
        };
        EngineMetrics metrics = new EngineMetrics(new SimpleMeterRegistry());
        Dispatcher dispatcher = new Dispatcher(leaseStore, jobStore, handlerRegistry, clock, List.of(),
                List.of(listenerBean), MohsExecutors.ioBoundExecutor("mohs-events-notify-test", 16), metrics);
        // piso == teto == 2s: sem NOTIFY, depois do 1º tick (vazio) o claim
        // seguinte só viria em 2s — é o que torna o await de 1s uma prova
        EngineSettings settings = new EngineSettings(Duration.ofSeconds(2), Duration.ofSeconds(2), 10, 10, 1,
                Duration.ofSeconds(30), Duration.ofSeconds(30), null, EngineSettings.DEFAULT_MISFIRE_THRESHOLD);
        engine = new Engine(workQueue, dispatcher, historyStore, leaseStore, jobStore, nodeStore,
                new JdbcTriggerFirer(dataSource, historyStore, workQueue), new ExecutionWindowRegistry(List.of()),
                rateLimitStore, clock, settings, new RunnerRegistry(List.of(MohsRunner.io("io").maxConcurrent(10).build())),
                metrics);
    }

    @AfterEach
    void tearDown() {
        if (listener != null) {
            listener.close();
            // mesmo racional do PostgresNotifyListenerTest: o próximo teste no
            // container compartilhado precisa de foto limpa de pg_stat_activity
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (listenerCount() > 0 && System.nanoTime() < deadline) {
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        if (engine != null) {
            engine.stop(Duration.ofSeconds(5));
        }
    }

    @Test
    void aCrossNodeOfferDispatchesThroughNotifyBeforeThePollWouldFire() throws Exception {
        engine.start();
        // baseline antes do start: o container é compartilhado e um backend de
        // listener de classe anterior ainda pode constar no pg_stat_activity
        int baseline = listenerCount();
        listener = new PostgresNotifyListener(PostgresTestSupport.jdbcUrl(), PostgresTestSupport.username(),
                PostgresTestSupport.password(), engine::ownsShard, engine::signalWorkScheduled);
        listener.start();
        awaitListenRegistered(baseline);

        // o "outro nó": história + fila por um caminho de conexão separado,
        // como o enqueue de um par faria — o offer emite o NOTIFY na mesma
        // transação e o listener deste nó converte em wake
        JdbcWorkQueue remoteQueue = new JdbcWorkQueue(dataSource, new PostgresJdbcDialect(),
                new JdbcBatchStore(dataSource, clock), clock);
        JdbcHistoryStore remoteHistory = new JdbcHistoryStore(dataSource, JsonMapper.builder().build(), new PostgresJdbcDialect());
        ExecutionId id = ExecutionId.of("exec-cross-node-1");
        int shard = Shards.of(id);
        remoteHistory.record(List.of(new HistoryStore.NewExecution(id, JobKey.of("welcome-email"), shard, 20,
                NOW.minusSeconds(1), NOW, "test", null, null, "hello")));
        remoteQueue.offer(List.of(new WorkQueue.ReadyEntry(id, JobKey.of("welcome-email"), shard, 20, 1, NOW.minusSeconds(1))));

        assertThat(succeeded.await(1, TimeUnit.SECONDS)).isTrue();
    }

    private void awaitListenRegistered(int baseline) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadline) {
            if (listenerCount() > baseline) {
                return;
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError("interrupted while waiting for the LISTEN registration", e);
            }
        }
        throw new AssertionError("LISTEN was never registered on the server");
    }

    private int listenerCount() {
        Integer listeners = new JdbcTemplate(dataSource).queryForObject(
                "SELECT COUNT(*) FROM pg_stat_activity WHERE query ILIKE 'LISTEN%'", Integer.class);
        return listeners == null ? 0 : listeners;
    }
}
