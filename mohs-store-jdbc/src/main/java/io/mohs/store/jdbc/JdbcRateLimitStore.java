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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import io.mohs.core.RateLimitSnapshot;
import io.mohs.core.resource.RateLimit;
import io.mohs.engine.RateLimitStore;
import io.mohs.store.jdbc.delegate.JdbcDelegate;

/** {@link RateLimitStore} over {@code mohs_rate_limits} (a Data Mapper, PoEAA), including the token bucket. */
public final class JdbcRateLimitStore implements RateLimitStore {

    private static final Logger log = LoggerFactory.getLogger(JdbcRateLimitStore.class);

    /** Two-second statement/transaction deadline for shared bucket contention. */
    static final Duration BUCKET_LOCK_TIMEOUT = Duration.ofSeconds(2);

    /** Bounded retries of the full bucket CAS; exhausted contention rejects admission. */
    private static final int MAX_CHARGE_ATTEMPTS = 3;

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final org.springframework.transaction.support.TransactionTemplate transaction;
    private final JdbcDelegate delegate;
    private final Set<String> unknownLimitsAlreadyWarned = ConcurrentHashMap.newKeySet();

    /**
     * Creates a {@code JdbcRateLimitStore} with the supplied values.
     *
     * @param dataSource the configured database connection source
     * @param delegate the database-specific SQL and timestamp adapter
     */
    public JdbcRateLimitStore(DataSource dataSource, JdbcDelegate delegate) {
        // Its own template, not JdbcSupport.namedTemplateWithStreamFetchSize: this store's ceiling is its
        // own (BUCKET_LOCK_TIMEOUT, the one unconditional lock wait on the claim path), tighter than the
        // tick's. The convention's fetch size stays — findAll returns a Stream.
        JdbcTemplate template = new JdbcTemplate(Objects.requireNonNull(dataSource, "dataSource"));
        template.setFetchSize(JdbcSupport.STREAM_FETCH_SIZE);
        template.setQueryTimeout((int) BUCKET_LOCK_TIMEOUT.toSeconds());
        this.jdbcTemplate = new NamedParameterJdbcTemplate(template);
        transaction = new org.springframework.transaction.support.TransactionTemplate(
                new org.springframework.jdbc.datasource.DataSourceTransactionManager(dataSource));
        transaction.setPropagationBehavior(org.springframework.transaction.TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        transaction.setIsolationLevel(org.springframework.transaction.TransactionDefinition.ISOLATION_READ_COMMITTED);
        transaction.setTimeout((int) BUCKET_LOCK_TIMEOUT.toSeconds());
        this.delegate = Objects.requireNonNull(delegate, "delegate");
    }

    /**
     * The UPDATE touches only the spec and clamps the balance to the new ceiling; the INSERT is born
     * with a FULL bucket — a freshly declared limit has no consumption history to charge for, and
     * starting empty would make the first job wait a whole window for a limit that was never exceeded.
     */
    @Override
    public RateLimit upsert(RateLimit rateLimit) {
        Objects.requireNonNull(rateLimit, "rateLimit");
        for (int attempt = 0; attempt < MAX_CHARGE_ATTEMPTS; attempt++) {
            try {
                boolean applied = Boolean.TRUE.equals(transaction.execute(_ -> {
                    var stored = readBucket(rateLimit.name());
                    long window = io.mohs.core.TimePrecision.micros(rateLimit.window());
                    if (stored.isPresent() && stored.orElseThrow().max() == rateLimit.max()
                            && stored.orElseThrow().windowMicros() == window) return true;
                    Instant now = JdbcDatabaseTime.now(jdbcTemplate, delegate);
                    var params = new MapSqlParameterSource("name", rateLimit.name())
                            .addValue("maxCount", rateLimit.max()).addValue("windowMicros", window)
                            .addValue("refilledAt", JdbcTimestamps.toUtcLocalDateTime(now));
                    if (stored.isEmpty()) {
                        return jdbcTemplate.update(delegate.insertFullRateLimitBucket(), params) == 1;
                    }
                    Bucket expected = stored.orElseThrow();
                    // A backward database clock cannot reopen an interval already accounted for.
                    if (now.isBefore(expected.refilledAt())) params.addValue("refilledAt", JdbcTimestamps.toUtcLocalDateTime(expected.refilledAt()));
                    params.addValue("tokens", Math.min(expected.tokens(), rateLimit.max()))
                            .addValue("expectedTokens", expected.tokens())
                            .addValue("expectedRefilledAt", JdbcTimestamps.toUtcLocalDateTime(expected.refilledAt()))
                            .addValue("expectedMax", expected.max()).addValue("expectedWindow", expected.windowMicros())
                            .addValue("expectedRemainder", expected.remainder());
                    return jdbcTemplate.update(delegate.updateRateLimitSpec(), params) == 1;
                }));
                if (applied) return rateLimit;
            } catch (DuplicateKeyException _) {
                // The competing birth won. Its failed transaction is rolled back before the next read.
            }
        }
        throw new org.springframework.dao.CannotAcquireLockException("rate-limit configuration changed concurrently: " + rateLimit.name());
    }

    @Override
    public Optional<RateLimitSnapshot> find(String name) {
        Objects.requireNonNull(name, "name");
        Instant now = JdbcDatabaseTime.now(jdbcTemplate, delegate);
        return JdbcSupport.findOne(jdbcTemplate,
                delegate.findRateLimitByName(),
                new MapSqlParameterSource("name", name),
                rs -> toSnapshot(rs, now));
    }

    @Override
    public Stream<RateLimitSnapshot> findAll() {
        Instant now = JdbcDatabaseTime.now(jdbcTemplate, delegate);
        return jdbcTemplate.queryForStream(delegate.findAllRateLimits(), new MapSqlParameterSource(),
                (rs, _) -> toSnapshot(rs, now));
    }

    /**
     * The refill is applied in MEMORY at read time: the row only stores the balance as of the last
     * charge, and showing that raw number would make the dashboard display an empty bucket long after it
     * had refilled. Writing here would be worse — it would turn a monitoring read into contention for
     * the claim hot path's lock.
     */
    private static RateLimitSnapshot toSnapshot(ResultSet rs, Instant now) throws SQLException {
        RateLimit rateLimit = mapRow(rs);
        // Clamped ONLY here: a tampered row (tokens > max_count) must not take down the other limits'
        // GET /rate-limits — and in this read the number guards no CAS, unlike mapBucket's raw balance.
        return new RateLimitSnapshot(rateLimit, Math.min(mapBucket(rs).refill(now).tokens(), rateLimit.max()));
    }

    /** A read with no lock at all: phase 1 must not cost serialisation, which is precisely what the revised design bought. */
    @Override
    public int available(String name) {
        Objects.requireNonNull(name, "name");
        Optional<Bucket> stored = readBucket(name);
        if (stored.isEmpty()) {
            warnOnceAboutUnknownLimit(name);
            return 0;
        }
        return stored.get().refill(JdbcDatabaseTime.now(jdbcTemplate, delegate)).tokens();
    }

    /** Own READ_COMMITTED transaction: read, database-time refill, full configuration/balance CAS.
     * A lost response is an unknown outcome; it cannot authorize dispatch or imply a refund. */
    @Override
    public boolean charge(String name, int permits) {
        Objects.requireNonNull(name, "name");
        if (permits < 1) throw new IllegalArgumentException("permits must be at least 1, got " + permits);
        return Boolean.TRUE.equals(transaction.execute(_ -> {
            for (int attempt = 0; attempt < MAX_CHARGE_ATTEMPTS; attempt++) {
                Optional<Bucket> stored = readBucket(name);
                if (stored.isEmpty()) {
                    warnOnceAboutUnknownLimit(name);
                    return false;
                }
                Bucket expected = stored.orElseThrow();
                Bucket refilled = expected.refill(JdbcDatabaseTime.now(jdbcTemplate, delegate));
                if (refilled.tokens() < permits) return false;
                if (chargeIfUnchanged(name, expected, refilled, permits)) return true;
            }
            return false;
        }));
    }

    /** Guard every input to refill: capacity, window, tokens, refilled time and fractional remainder. */
    private boolean chargeIfUnchanged(String name, Bucket expected, Bucket refilled, int permits) {
        return jdbcTemplate.update(delegate.chargeRateLimitByCas(), new MapSqlParameterSource("name", name)
                .addValue("tokens", refilled.tokens() - permits)
                .addValue("refilledAt", JdbcTimestamps.toUtcLocalDateTime(refilled.refilledAt()))
                .addValue("expectedTokens", expected.tokens())
                .addValue("expectedRefilledAt", JdbcTimestamps.toUtcLocalDateTime(expected.refilledAt()))
                .addValue("expectedMax", expected.max())
                .addValue("expectedWindow", expected.windowMicros())
                .addValue("remainder", refilled.remainder())
                .addValue("expectedRemainder", expected.remainder())) == 1;
    }

    private Optional<Bucket> readBucket(String name) {
        return JdbcSupport.findOne(jdbcTemplate,
                delegate.readRateLimitBucket(),
                new MapSqlParameterSource("name", name),
                JdbcRateLimitStore::mapBucket);
    }

    /**
     * Once per name, not once per round: {@link #available} runs on every claim (20 times a second per
     * node at the {@code poll=50ms} operating point), and a single job with the wrong name would turn
     * the WARN that ought to draw attention at 3 a.m. into the flood that hides everything else in the
     * log. The set is bounded by the names jobs actually reference — it does not grow with traffic.
     */
    private void warnOnceAboutUnknownLimit(String name) {
        if (unknownLimitsAlreadyWarned.add(name)) {
            log.warn("job references unknown rate limit '{}' — granting nothing (fail-safe) until it is declared "
                    + "with mohs.rate-limits.{}.max/.window or a @Bean RateLimit", name, name);
        }
    }

    private static RateLimit mapRow(ResultSet rs) throws SQLException {
        return new RateLimit(rs.getString("name"), rs.getInt("max_count"), io.mohs.core.TimePrecision.duration(rs.getLong("window_micros")));
    }

    private static Bucket mapBucket(ResultSet rs) throws SQLException {
        int max = rs.getInt("max_count");
        long window = rs.getLong("window_micros");
        long remainder = rs.getLong("refill_remainder");
        if (max < 1 || window < 1 || remainder < 0 || remainder >= window) {
            throw new IllegalStateException("invalid persisted rate bucket: positive capacity/window and fraction within window required");
        }
        return new Bucket(max, window, rs.getInt("tokens"),
                JdbcTimestamps.fromUtcLocalDateTime(rs.getObject("refilled_at", LocalDateTime.class)), remainder);
    }

    /** Fractional token credit has numerator microseconds * capacity and denominator windowMicros. */
    private record Bucket(int max, long windowMicros, int tokens, Instant refilledAt, long remainder) {
        Bucket refill(Instant observed) {
            Instant now = io.mohs.core.TimePrecision.observed(observed);
            if (tokens > max) return new Bucket(max, windowMicros, max, now.isAfter(refilledAt) ? now : refilledAt, 0);
            if (!now.isAfter(refilledAt)) return this;
            long elapsed = io.mohs.core.TimePrecision.micros(Duration.between(refilledAt, now));
            if (tokens >= max || elapsed >= windowMicros) return new Bucket(max, windowMicros, max, now, 0);
            long earned;
            long fraction;
            try {
                long credit = Math.addExact(Math.multiplyExact(elapsed, max), remainder);
                earned = credit / windowMicros;
                fraction = credit % windowMicros;
            } catch (ArithmeticException largeWindow) {
                var quotient = java.math.BigInteger.valueOf(elapsed).multiply(java.math.BigInteger.valueOf(max))
                        .add(java.math.BigInteger.valueOf(remainder)).divideAndRemainder(java.math.BigInteger.valueOf(windowMicros));
                earned = quotient[0].longValueExact();
                fraction = quotient[1].longValueExact();
            }
            if (earned >= max - tokens) return new Bucket(max, windowMicros, max, now, 0);
            return new Bucket(max, windowMicros, tokens + (int) earned, now, fraction);
        }
    }
}
