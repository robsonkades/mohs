package io.mohs.store.jdbc;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import io.mohs.core.resource.RateLimit;
import io.mohs.store.jdbc.delegate.JdbcDelegate;
import io.mohs.test.MutableClock;
import static org.assertj.core.api.Assertions.assertThat;

class JdbcRateConfigurationProtocolTest {
    private static final Instant NOW = Instant.parse("2026-09-10T00:00:00Z");

    @ParameterizedTest(name = "{0}")
    @MethodSource("io.mohs.store.jdbc.JdbcJobBindingTest#backends")
    void aChargeCalculatedBeforeReconfigurationCannotUseTheOldRefillRate(JdbcJobBindingTest.Backend backend) throws Exception {
        var clock = MutableClock.startingAt(NOW);
        var dialect = TestDatabaseClock.at(backend.delegate(), clock);
        var store = new JdbcRateLimitStore(backend.dataSource(), dialect);
        store.upsert(new RateLimit("shared", 10, Duration.ofSeconds(1)));
        assertThat(store.charge("shared", 10)).isTrue();
        clock.advance(Duration.ofSeconds(1));
        var calculated = new CountDownLatch(1);
        var continueCharge = new CountDownLatch(1);
        var delayed = new JdbcRateLimitStore(backend.dataSource(), pauseOnce(dialect, "chargeRateLimitByCas", calculated, continueCharge));
        try (var threads = Executors.newVirtualThreadPerTaskExecutor()) {
            var charge = threads.submit(() -> delayed.charge("shared", 10));
            try {
                assertThat(calculated.await(5, TimeUnit.SECONDS)).isTrue();
                store.upsert(new RateLimit("shared", 10, Duration.ofHours(1)));
            } finally { continueCharge.countDown(); }
            assertThat(charge.get(5, TimeUnit.SECONDS)).isFalse();
        }
        assertThat(store.available("shared")).isZero();
        clock.advance(Duration.ofHours(1));
        assertThat(store.charge("shared", 10)).isTrue();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("io.mohs.store.jdbc.JdbcJobBindingTest#backends")
    void losingBucketBirthRollsBackBeforeReconciliationAndDoesNotResetConsumption(JdbcJobBindingTest.Backend backend) throws Exception {
        var clock = MutableClock.startingAt(NOW);
        var dialect = TestDatabaseClock.at(backend.delegate(), clock);
        var beforeInsert = new CountDownLatch(1);
        var continueInsert = new CountDownLatch(1);
        var delayed = new JdbcRateLimitStore(backend.dataSource(), pauseOnce(dialect, "insertFullRateLimitBucket", beforeInsert, continueInsert));
        var store = new JdbcRateLimitStore(backend.dataSource(), dialect);
        var desired = new RateLimit("shared", 5, Duration.ofHours(1));
        try (var threads = Executors.newVirtualThreadPerTaskExecutor()) {
            var insertion = threads.submit(() -> delayed.upsert(desired));
            try {
                assertThat(beforeInsert.await(5, TimeUnit.SECONDS)).isTrue();
                store.upsert(desired);
                assertThat(store.charge("shared", 3)).isTrue();
            } finally { continueInsert.countDown(); }
            assertThat(insertion.get(5, TimeUnit.SECONDS)).isEqualTo(desired);
        }
        assertThat(store.available("shared")).isEqualTo(2);
    }

    private static JdbcDelegate pauseOnce(JdbcDelegate delegate, String methodName, CountDownLatch arrived, CountDownLatch released) {
        var first = new AtomicBoolean(true);
        return (JdbcDelegate) Proxy.newProxyInstance(delegate.getClass().getClassLoader(), new Class<?>[]{JdbcDelegate.class},
                (_, method, args) -> {
                    if (method.getName().equals(methodName) && first.compareAndSet(true, false)) {
                        arrived.countDown();
                        if (!released.await(5, TimeUnit.SECONDS)) throw new AssertionError("interleaving barrier not released");
                    }
                    try { return method.invoke(delegate, args); }
                    catch (InvocationTargetException failure) { throw failure.getCause(); }
                });
    }
}
