package io.mohs.jdbc;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.sql.DataSource;

import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.transaction.support.TransactionTemplate;

import io.mohs.core.RateLimitSnapshot;
import io.mohs.core.resource.RateLimit;
import io.mohs.jdbc.dialect.H2JdbcDialect;
import io.mohs.test.MutableClock;

import static org.assertj.core.api.Assertions.assertThat;

class JdbcRateLimitStoreTest {

    private static final Instant NOW = Instant.parse("2026-08-18T12:00:00Z");

    private DataSource dataSource;
    private JdbcRateLimitStore store;
    private MutableClock clock;

    @BeforeEach
    void setUp() {
        clock = new MutableClock(NOW, ZoneId.of("UTC"));
        dataSource = freshH2DataSource();
        store = new JdbcRateLimitStore(dataSource, clock);
    }

    private static DataSource freshH2DataSource() {
        JdbcDataSource h2 = new JdbcDataSource();
        h2.setURL("jdbc:h2:mem:rate-limit-store-test-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        h2.setUser("sa");
        h2.setPassword("");
        new ResourceDatabasePopulator(new ClassPathResource("schema-h2.sql")).execute(h2);
        return h2;
    }

    @Test
    void upsertInsertsANewRateLimit() {
        RateLimit rateLimit = new RateLimit("smtp", 100, Duration.ofMinutes(1));

        store.upsert(rateLimit);

        assertThat(store.find("smtp").map(RateLimitSnapshot::rateLimit)).contains(rateLimit);
    }

    @Test
    void upsertOnExistingNameAppliesChanges() {
        store.upsert(new RateLimit("smtp", 100, Duration.ofMinutes(1)));

        store.upsert(new RateLimit("smtp", 250, Duration.ofSeconds(30)));

        assertThat(store.find("smtp").map(RateLimitSnapshot::rateLimit)).contains(new RateLimit("smtp", 250, Duration.ofSeconds(30)));
    }

    @Test
    void findReturnsEmptyForUnknownName() {
        assertThat(store.find("ghost")).isEmpty();
    }

    @Test
    void findAllReturnsEveryRateLimit() {
        store.upsert(new RateLimit("smtp", 100, Duration.ofMinutes(1)));
        store.upsert(new RateLimit("partner-api", 50, Duration.ofSeconds(10)));

        try (var all = store.findAll()) {
            assertThat(all.map(snapshot -> snapshot.rateLimit().name())).containsExactlyInAnyOrder("smtp", "partner-api");
        }
    }

    @Test
    void aNewRateLimitIsBornWithAFullBucket() {
        store.upsert(new RateLimit("smtp", 100, Duration.ofMinutes(1)));

        assertThat(store.find("smtp")).get().extracting(RateLimitSnapshot::available).isEqualTo(100);
    }

    @Test
    void chargeTakesThePermitsFromTheBucket() {
        store.upsert(new RateLimit("smtp", 100, Duration.ofMinutes(1)));

        assertThat(store.charge("smtp", 30, clock.instant())).isTrue();
        assertThat(store.find("smtp")).get().extracting(RateLimitSnapshot::available).isEqualTo(70);
    }

    /**
     * {@code charge} é tudo ou nada: quem decide QUANTO pedir é a fase 1
     * ({@code available}), no claimer. Cobrar parcial aqui seria entregar
     * menos tokens do que as execuções já reivindicadas na transação —
     * exatamente a sobre-entrega que o CAS existe pra impedir.
     */
    @Test
    void chargeIsAllOrNothingAgainstTheRemainingBalance() {
        store.upsert(new RateLimit("smtp", 100, Duration.ofMinutes(1)));
        store.charge("smtp", 90, clock.instant());

        assertThat(store.available("smtp", clock.instant())).isEqualTo(10);
        assertThat(store.charge("smtp", 50, clock.instant())).isFalse();
        assertThat(store.charge("smtp", 10, clock.instant())).isTrue();
    }

    /** Um token a cada window/max (600ms para 100/min): meio intervalo não rende token nenhum. */
    @Test
    void theBucketRefillsOneTokenPerIntervalAndNotBefore() {
        store.upsert(new RateLimit("smtp", 100, Duration.ofMinutes(1)));
        store.charge("smtp", 100, clock.instant());

        clock.advance(Duration.ofMillis(300));
        assertThat(store.charge("smtp", 1, clock.instant())).isFalse();

        clock.advance(Duration.ofMillis(300));
        assertThat(store.charge("smtp", 1, clock.instant())).isTrue();
    }

    /**
     * A fração pendente sobrevive: sem guardar o resto da divisão, cada
     * chamada descartaria o tempo não convertido e o limite entregaria
     * menos que {@code max} por janela para sempre.
     */
    @Test
    void refillKeepsTheLeftoverTimeBetweenCalls() {
        store.upsert(new RateLimit("smtp", 100, Duration.ofMinutes(1)));
        store.charge("smtp", 100, clock.instant());

        // três avanços de 400ms = 1200ms = 2 tokens de 600ms; o consumo em
        // 800ms leva o primeiro, então sobra 1. Descartar a fração a cada
        // chamada (refilledAt = "agora" em vez de += refill × intervalo)
        // daria 0 aqui: os 200ms restantes de cada passo evaporariam.
        clock.advance(Duration.ofMillis(400));
        store.charge("smtp", 1, clock.instant());
        clock.advance(Duration.ofMillis(400));
        store.charge("smtp", 1, clock.instant());
        clock.advance(Duration.ofMillis(400));

        assertThat(store.find("smtp")).get().extracting(RateLimitSnapshot::available).isEqualTo(1);
    }

    @Test
    void theBucketNeverRefillsBeyondItsCapacity() {
        store.upsert(new RateLimit("smtp", 100, Duration.ofMinutes(1)));
        store.charge("smtp", 100, clock.instant());

        clock.advance(Duration.ofHours(3));

        assertThat(store.find("smtp")).get().extracting(RateLimitSnapshot::available).isEqualTo(100);
    }

    /** Relógio para trás atrasa a liberação; jamais libera dobrado (ADR-0042). */
    @Test
    void aClockGoingBackwardsRefillsNothing() {
        store.upsert(new RateLimit("smtp", 100, Duration.ofMinutes(1)));
        store.charge("smtp", 100, clock.instant());

        assertThat(store.charge("smtp", 1, NOW.minus(Duration.ofHours(1)))).isFalse();
    }

    /** Fail-safe da ADR-0042: nome inexistente concede zero em vez de deixar passar sem limite. */
    @Test
    void anUnknownRateLimitGrantsNothingAndChargesNothing() {
        assertThat(store.available("ghost", clock.instant())).isZero();
        assertThat(store.charge("ghost", 10, clock.instant())).isFalse();
    }

    /**
     * O balde é estado operacional e sobrevive ao boot — senão cada nó
     * subindo num rolling deploy devolveria um balde cheio e o deploy
     * viraria burst.
     */
    @Test
    void upsertKeepsTheBucketBalance() {
        store.upsert(new RateLimit("smtp", 100, Duration.ofMinutes(1)));
        store.charge("smtp", 60, clock.instant());

        store.upsert(new RateLimit("smtp", 100, Duration.ofMinutes(1)));

        assertThat(store.find("smtp")).get().extracting(RateLimitSnapshot::available).isEqualTo(40);
    }

    @Test
    void loweringMaxClampsABucketThatHeldMoreThanTheNewCeiling() {
        store.upsert(new RateLimit("smtp", 100, Duration.ofMinutes(1)));

        store.upsert(new RateLimit("smtp", 10, Duration.ofMinutes(1)));

        assertThat(store.find("smtp")).get().extracting(RateLimitSnapshot::available).isEqualTo(10);
    }

    /**
     * Linha adulterada com saldo acima do teto — cenário real: a ADR-0042
     * manda o operador rodar DDL manual nesta tabela no upgrade. Clampar o
     * saldo em memória cegaria o CAS ({@code expectedTokens} nunca casaria
     * com a linha) e o limite ficaria incobrável PARA SEMPRE, derrubando toda
     * rodada que o tocasse. Aqui o clamp vive só na leitura de dashboard.
     */
    @Test
    void chargeStillWorksOnARowHoldingMoreTokensThanItsCeiling() {
        store.upsert(new RateLimit("smtp", 10, Duration.ofMinutes(1)));
        new JdbcTemplate(dataSource).update("UPDATE mohs_rate_limits SET tokens = 50 WHERE name = 'smtp'");

        // o excedente adulterado é descartado (clamp para a capacidade de 10),
        // a cobrança de 5 é honrada, sobram 5 — e o limite continua cobrável,
        // que é o ponto: com clamp em mapBucket, o CAS nunca mais casaria
        assertThat(store.charge("smtp", 5, clock.instant())).isTrue();
        assertThat(store.find("smtp")).get().extracting(RateLimitSnapshot::available).isEqualTo(5);
    }

    /**
     * O texto de {@code window_duration} escrito à mão numa forma
     * equivalente mas não canônica ({@code PT60S} em vez de {@code PT1M}) não
     * pode travar a cobrança: o CAS compara TEXTO, e re-serializar o
     * {@code Duration} parseado faria o predicado nunca casar.
     */
    @Test
    void chargeWorksWhenTheStoredWindowTextIsNotCanonical() {
        store.upsert(new RateLimit("smtp", 100, Duration.ofMinutes(1)));
        new JdbcTemplate(dataSource).update("UPDATE mohs_rate_limits SET window_duration = 'PT60S' WHERE name = 'smtp'");

        assertThat(store.charge("smtp", 1, clock.instant())).isTrue();
    }

    /** Leitura é pura: consultar o saldo não pode consumir nem mover o balde. */
    @Test
    void findDoesNotConsumeTokens() {
        store.upsert(new RateLimit("smtp", 100, Duration.ofMinutes(1)));

        store.find("smtp");
        store.find("smtp");

        assertThat(store.charge("smtp", 100, clock.instant())).isTrue();
    }

    /**
     * A afirmação central do mecanismo: duas transações concorrentes pedindo
     * o balde inteiro não podem somar mais que a capacidade — sobre-entrega é
     * a ÚNICA violação inaceitável do contrato (ADR-0042). Quem garante isso
     * é o CAS sobre {@code (tokens, refilled_at)} dentro de {@code charge},
     * não lock pessimista (revisão de duas fases, 2026-08-18): sem ele as
     * duas leriam 10 e cobrariam 20. Vale como prova porque as duas rodam em
     * transações de verdade, disputando a mesma linha via barrier — mesmo
     * padrão de {@code upsertHandlesConcurrentFirstTimeInsertWithoutThrowing}.
     */
    @Test
    void twoConcurrentConsumersNeverGrantMoreThanTheBucketHolds() throws Exception {
        store.upsert(new RateLimit("smtp", 10, Duration.ofMinutes(1)));
        TransactionTemplate transaction = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
        CyclicBarrier barrier = new CyclicBarrier(2);
        Callable<Integer> round = () -> transaction.execute(_ -> {
            awaitQuietly(barrier);
            return store.charge("smtp", 10, clock.instant()) ? 10 : 0;
        });
        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

        Future<Integer> a = executor.submit(round);
        Future<Integer> b = executor.submit(round);
        int granted = a.get(10, TimeUnit.SECONDS) + b.get(10, TimeUnit.SECONDS);
        executor.shutdown();
        assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();

        assertThat(granted).isEqualTo(10);
        assertThat(store.find("smtp")).get().extracting(RateLimitSnapshot::available).isEqualTo(0);
    }

    private static void awaitQuietly(CyclicBarrier barrier) {
        try {
            barrier.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while lining up the concurrent consumers", e);
        } catch (BrokenBarrierException | TimeoutException e) {
            throw new IllegalStateException("the concurrent consumers never lined up", e);
        }
    }

    /** CONC-2 — ver JdbcJobStoreTest.upsertHandlesConcurrentFirstTimeInsertWithoutThrowing. */
    @Test
    void upsertHandlesConcurrentFirstTimeInsertWithoutThrowing() throws Exception {
        RateLimit rateLimit = new RateLimit("smtp", 100, Duration.ofMinutes(1));
        CyclicBarrier barrier = new CyclicBarrier(2);
        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        Callable<RateLimit> upsert = () -> {
            barrier.await();
            return store.upsert(rateLimit);
        };

        Future<RateLimit> futureA = executor.submit(upsert);
        Future<RateLimit> futureB = executor.submit(upsert);
        futureA.get(10, TimeUnit.SECONDS);
        futureB.get(10, TimeUnit.SECONDS);
        executor.shutdown();
        assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();

        assertThat(store.find("smtp").map(RateLimitSnapshot::rateLimit)).contains(rateLimit);
    }
}
