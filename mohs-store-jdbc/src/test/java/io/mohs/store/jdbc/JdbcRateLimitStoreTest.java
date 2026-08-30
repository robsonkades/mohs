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
package io.mohs.store.jdbc;

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
import io.mohs.store.jdbc.dialect.H2JdbcDialect;
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
     * {@code charge} is all or nothing: what decides HOW MUCH to ask for is phase 1 ({@code available}),
     * in the claimer. Charging partially here would deliver fewer tokens than the executions already
     * claimed in the transaction — exactly the over-delivery the CAS exists to prevent.
     */
    @Test
    void chargeIsAllOrNothingAgainstTheRemainingBalance() {
        store.upsert(new RateLimit("smtp", 100, Duration.ofMinutes(1)));
        store.charge("smtp", 90, clock.instant());

        assertThat(store.available("smtp", clock.instant())).isEqualTo(10);
        assertThat(store.charge("smtp", 50, clock.instant())).isFalse();
        assertThat(store.charge("smtp", 10, clock.instant())).isTrue();
    }

    /** One token every window/max (600ms for 100/min): half an interval yields no token at all. */
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
     * The pending fraction survives: without keeping the division's remainder, each call would discard the
     * unconverted time and the limit would deliver less than {@code max} per window forever.
     */
    @Test
    void refillKeepsTheLeftoverTimeBetweenCalls() {
        store.upsert(new RateLimit("smtp", 100, Duration.ofMinutes(1)));
        store.charge("smtp", 100, clock.instant());

        // Three advances of 400ms make 1200ms, which is 2 tokens of 600ms; the consumption at 800ms takes
        // the first, so 1 remains. Discarding the fraction on every call (refilledAt = "now" rather than
        // += refill x interval) would give 0 here: each step's remaining 200ms would evaporate.
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

    /** A clock running backwards delays the release; it never releases twice as much. */
    @Test
    void aClockGoingBackwardsRefillsNothing() {
        store.upsert(new RateLimit("smtp", 100, Duration.ofMinutes(1)));
        store.charge("smtp", 100, clock.instant());

        assertThat(store.charge("smtp", 1, NOW.minus(Duration.ofHours(1)))).isFalse();
    }

    /** The fail-safe: a nonexistent name grants zero rather than letting work through with no limit. */
    @Test
    void anUnknownRateLimitGrantsNothingAndChargesNothing() {
        assertThat(store.available("ghost", clock.instant())).isZero();
        assertThat(store.charge("ghost", 10, clock.instant())).isFalse();
    }

    /**
     * The bucket is operational state and survives a boot — otherwise every node coming up in a rolling
     * deploy would hand back a full bucket and the deploy would become a burst.
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
     * A tampered row with a balance above the ceiling — a real scenario: the operator is told to run manual
     * DDL on this table during an upgrade. Clamping the balance in memory would blind the CAS
     * ({@code expectedTokens} would never match the row) and the limit would become uncharge­able FOREVER,
     * taking down every round that touched it. Here the clamp lives only in the dashboard read.
     */
    @Test
    void chargeStillWorksOnARowHoldingMoreTokensThanItsCeiling() {
        store.upsert(new RateLimit("smtp", 10, Duration.ofMinutes(1)));
        new JdbcTemplate(dataSource).update("UPDATE mohs_rate_limits SET tokens = 50 WHERE name = 'smtp'");

        // The tampered surplus is discarded (clamped to the capacity of 10), the charge of 5 is honoured and
        // 5 remain — and the limit stays chargeable, which is the point: with a clamp in mapBucket, the CAS
        // would never match again
        assertThat(store.charge("smtp", 5, clock.instant())).isTrue();
        assertThat(store.find("smtp")).get().extracting(RateLimitSnapshot::available).isEqualTo(5);
    }

    /**
     * A {@code window_duration} text written by hand in an equivalent but non-canonical form
     * ({@code PT60S} instead of {@code PT1M}) must not stall the charge: the CAS compares TEXT, and
     * re-serialising the parsed {@code Duration} would make the predicate never match.
     */
    @Test
    void chargeWorksWhenTheStoredWindowTextIsNotCanonical() {
        store.upsert(new RateLimit("smtp", 100, Duration.ofMinutes(1)));
        new JdbcTemplate(dataSource).update("UPDATE mohs_rate_limits SET window_duration = 'PT60S' WHERE name = 'smtp'");

        assertThat(store.charge("smtp", 1, clock.instant())).isTrue();
    }

    /** A read is pure: checking the balance must neither consume nor move the bucket. */
    @Test
    void findDoesNotConsumeTokens() {
        store.upsert(new RateLimit("smtp", 100, Duration.ofMinutes(1)));

        store.find("smtp");
        store.find("smtp");

        assertThat(store.charge("smtp", 100, clock.instant())).isTrue();
    }

    /**
     * The mechanism's central claim: two concurrent transactions each asking for the whole bucket cannot
     * add up to more than the capacity — over-delivery is the ONLY unacceptable violation of the contract.
     *
     * <p>What guarantees that is the CAS over {@code (tokens, refilled_at)} inside {@code charge}, not a
     * pessimistic lock: without it, both would read 10 and charge 20. It counts as proof because both run
     * in real transactions, contending for the same row through a barrier — the same pattern as
     * {@code upsertHandlesConcurrentFirstTimeInsertWithoutThrowing}.
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
