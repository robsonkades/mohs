package io.mohs.store.jdbc;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.IntPredicate;

import javax.sql.DataSource;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import io.mohs.core.execution.ExecutionId;
import io.mohs.core.job.JobKey;
import io.mohs.engine.WorkQueue;
import io.mohs.store.jdbc.dialect.PostgresJdbcDialect;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tier 2 do wake-up (§5.5, S6.3) contra Postgres real: o NOTIFY nasce no
 * offer (mesma transação) e o LISTEN em conexão dedicada acorda quem
 * possui o shard — e SÓ quem possui. Queda da conexão degrada pro poll
 * (reconexão com backoff), nunca erro fatal.
 */
class PostgresNotifyListenerTest {

    private static final Instant NOW = Instant.parse("2026-08-15T12:00:00Z");

    private DataSource dataSource;
    private JdbcWorkQueue workQueue;
    private PostgresNotifyListener listener;

    @BeforeEach
    void setUp() {
        dataSource = PostgresTestSupport.freshSchema();
        Clock clock = Clock.fixed(NOW, ZoneOffset.UTC);
        workQueue = new JdbcWorkQueue(dataSource, new PostgresJdbcDialect(), new JdbcBatchStore(dataSource, clock), clock);
    }

    @AfterEach
    void tearDown() {
        if (listener != null) {
            listener.close();
            // espera o backend sumir do servidor: o container é compartilhado
            // entre classes e um LISTEN defunto na foto do próximo teste
            // corromperia a espera de registro dele
            awaitListenerCount(count -> count == 0, "the closed LISTEN connection never left pg_stat_activity");
        }
    }

    private PostgresNotifyListener startedListener(IntPredicate ownsShard, Runnable wake) {
        // baseline ANTES do start: o container é compartilhado entre classes e
        // o backend do listener de um teste anterior pode ainda constar no
        // pg_stat_activity — "count > 0" sozinho validaria o defunto e o
        // NOTIFY sairia no vácuo (review S6.3)
        int baseline = listenerCount();
        listener = new PostgresNotifyListener(PostgresTestSupport.jdbcUrl(), PostgresTestSupport.username(),
                PostgresTestSupport.password(), ownsShard, wake);
        listener.start();
        awaitListenerCount(count -> count > baseline, "LISTEN was never registered on the server");
        return listener;
    }

    /** Espera de registro no servidor — notificação emitida antes do LISTEN se perde por design (Postgres não enfileira pra quem ainda não escuta). */
    private void awaitListenerCount(IntPredicate condition, String failure) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() < deadline) {
            if (condition.test(listenerCount())) {
                return;
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError("interrupted while waiting for the LISTEN registration", e);
            }
        }
        throw new AssertionError(failure);
    }

    private int listenerCount() {
        Integer listeners = new JdbcTemplate(dataSource).queryForObject(
                "SELECT COUNT(*) FROM pg_stat_activity WHERE query ILIKE 'LISTEN%'", Integer.class);
        return listeners == null ? 0 : listeners;
    }

    private static WorkQueue.ReadyEntry entry(String id, int shard, Instant visibleAt) {
        return new WorkQueue.ReadyEntry(ExecutionId.of(id), JobKey.of("welcome-email"), shard, 20, 1, visibleAt);
    }

    @Test
    void aDueOfferNotifiesTheShardOwnerAcrossConnections() throws Exception {
        CountDownLatch woke = new CountDownLatch(1);
        startedListener(_ -> true, woke::countDown);

        workQueue.offer(List.of(entry("exec-notify-1", 7, NOW.minusSeconds(1))));

        assertThat(woke.await(5, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    void aNotificationForAForeignShardDoesNotWake() throws Exception {
        CountDownLatch woke = new CountDownLatch(1);
        startedListener(shard -> shard != 7, woke::countDown);

        workQueue.offer(List.of(entry("exec-notify-2", 7, NOW.minusSeconds(1))));

        assertThat(woke.await(700, TimeUnit.MILLISECONDS)).isFalse();
    }

    /** Entrada futura não emite NOTIFY (dueness decidida no offer): acordar um nó pra linha invisível seria um lap perdido — o poll a pega quando vencer. */
    @Test
    void aFutureOfferDoesNotNotify() throws Exception {
        CountDownLatch woke = new CountDownLatch(1);
        startedListener(_ -> true, woke::countDown);

        workQueue.offer(List.of(entry("exec-notify-3", 7, NOW.plusSeconds(3600))));

        assertThat(woke.await(700, TimeUnit.MILLISECONDS)).isFalse();
    }

    /**
     * P1 do S6.3 (conflação global por emissor): um shard cujo sinal caiu
     * dentro da janela fica PENDENTE e é carregado pelo próximo offer que
     * vencer a janela — conflação nunca vira perda enquanto houver
     * tráfego (a cauda sem tráfego é do poll, best-effort contratado). O
     * teste não pina QUAL offer emite (timing da janela de 50ms não é
     * determinístico em teste): pina que o sinal do shard 9 CHEGA, direto
     * ou carregado.
     */
    @Test
    void aConflatedShardIsCarriedByTheNextWindowWinnerNotLost() throws Exception {
        CountDownLatch shardNineSignalled = new CountDownLatch(1);
        startedListener(shard -> shard == 9, shardNineSignalled::countDown);

        workQueue.offer(List.of(entry("exec-conflate-1", 7, NOW.minusSeconds(1))));
        workQueue.offer(List.of(entry("exec-conflate-2", 9, NOW.minusSeconds(1))));
        // deixa a janela de 50ms vencer com folga: se o sinal do 9 caiu na
        // janela do 7, o offer seguinte vence uma janela nova e carrega o
        // pendente junto com o próprio 11
        Thread.sleep(120);
        workQueue.offer(List.of(entry("exec-conflate-3", 11, NOW.minusSeconds(1))));

        assertThat(shardNineSignalled.await(5, TimeUnit.SECONDS)).isTrue();
    }

    /** Best-effort de verdade: a conexão de LISTEN morta por fora reconecta sozinha (backoff) e volta a acordar — sem erro fatal no meio. */
    @Test
    void aKilledListenConnectionReconnectsAndKeepsWaking() throws Exception {
        CountDownLatch woke = new CountDownLatch(1);
        startedListener(_ -> true, woke::countDown);

        JdbcTemplate jdbc = new JdbcTemplate(dataSource);
        jdbc.query("SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                + "WHERE pid <> pg_backend_pid() AND query ILIKE 'LISTEN%'", _ -> { });
        // primeiro a conexão morta some, DEPOIS a reconexão re-registra o
        // LISTEN (backoff de 1s) — sem as duas esperas o NOTIFY abaixo podia
        // sair no vácuo entre as duas conexões e o teste flakearia
        awaitListenerCount(count -> count == 0, "the killed LISTEN connection never went away");
        awaitListenerCount(count -> count > 0, "the LISTEN connection never came back");

        workQueue.offer(List.of(entry("exec-notify-4", 7, NOW.minusSeconds(1))));

        assertThat(woke.await(5, TimeUnit.SECONDS)).isTrue();
    }
}
