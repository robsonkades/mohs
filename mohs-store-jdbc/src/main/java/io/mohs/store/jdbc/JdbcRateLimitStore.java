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
import java.time.Clock;
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

    /**
     * The ceiling on waiting for the bucket row's lock (today only {@link #charge}'s {@code UPDATE}
     * holds it, and only until the commit) — the ONLY unconditional wait on the claim path, which never
     * used to block at all (the candidate selection's {@code SKIP LOCKED}/{@code READPAST} skips what is
     * locked, never waits).
     *
     * <p>Without a ceiling, one stuck node holding the row delays OTHER nodes' ticks — and the tick is
     * what beats the heartbeat and renews the lease, so contention would become a false positive of
     * death and the reaper would reclaim executions that are still running: work duplicated by the very
     * mechanism meant to protect the external resource.
     *
     * <p>Once the ceiling expires, a {@code QueryTimeoutException} comes out — MEASURED on H2 (SQLState
     * 50200, 2013ms) and Postgres 18 (57014, 2022ms), not a {@code CannotAcquireLockException}: Spring's
     * translator sends statement timeout and deadlock to SIBLING branches of the hierarchy. That is why
     * the claim's caller catches both explicitly — the round is lost, never the heartbeat.
     *
     * <p>Two seconds assume a generous {@code lease-ttl} (30s by default): the wait has to fit
     * comfortably inside the TTL, otherwise the ceiling that protects the heartbeat becomes what
     * consumes it. A {@code lease-ttl} below about 10s calls for revisiting this value.
     */
    static final Duration BUCKET_LOCK_TIMEOUT = Duration.ofSeconds(2);

    /**
     * Attempts at {@link #charge}'s CAS before returning failure to the caller. Three, because the
     * asymmetric cost dictates it: each attempt costs two round trips, while giving up costs the whole
     * claim round (the CAS of up to {@code batchSize} executions).
     *
     * <p>A collision requires two nodes charging the SAME limit within the same microsecond window
     * between read and write — three attempts amply cover anything that is not pathological contention;
     * and if it is, the lost round is the right signal.
     */
    private static final int MAX_CHARGE_ATTEMPTS = 3;

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final Clock clock;
    private final JdbcDelegate delegate;
    private final Set<String> unknownLimitsAlreadyWarned = ConcurrentHashMap.newKeySet();

    public JdbcRateLimitStore(DataSource dataSource, Clock clock, JdbcDelegate delegate) {
        // Its own template, not JdbcSupport.namedTemplateWithStreamFetchSize: this store's ceiling is its
        // own (BUCKET_LOCK_TIMEOUT, the one unconditional lock wait on the claim path), tighter than the
        // tick's. The convention's fetch size stays — findAll returns a Stream.
        JdbcTemplate template = new JdbcTemplate(Objects.requireNonNull(dataSource, "dataSource"));
        template.setFetchSize(JdbcSupport.STREAM_FETCH_SIZE);
        template.setQueryTimeout((int) BUCKET_LOCK_TIMEOUT.toSeconds());
        this.jdbcTemplate = new NamedParameterJdbcTemplate(template);
        this.clock = Objects.requireNonNull(clock, "clock");
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
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("name", rateLimit.name())
                .addValue("maxCount", rateLimit.max())
                .addValue("windowDuration", rateLimit.window().toString())
                .addValue("refilledAt", JdbcTimestamps.toUtcLocalDateTime(clock.instant()));

        // See the equivalent race in JdbcJobStore.upsert — same race, same fix.
        int updated = jdbcTemplate.update(delegate.updateRateLimitSpec(), params);
        if (updated == 0) {
            try {
                jdbcTemplate.update(delegate.insertFullRateLimitBucket(), params);
            } catch (DuplicateKeyException _) {
                jdbcTemplate.update(delegate.updateRateLimitSpec(), params);
            }
        }
        return rateLimit;
    }

    @Override
    public Optional<RateLimitSnapshot> find(String name) {
        Objects.requireNonNull(name, "name");
        Instant now = clock.instant();
        return JdbcSupport.findOne(jdbcTemplate,
                delegate.findRateLimitByName(),
                new MapSqlParameterSource("name", name),
                rs -> toSnapshot(rs, now));
    }

    @Override
    public Stream<RateLimitSnapshot> findAll() {
        Instant now = clock.instant();
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
    public int available(String name, Instant now) {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(now, "now");
        Optional<Bucket> stored = readBucket(name);
        if (stored.isEmpty()) {
            warnOnceAboutUnknownLimit(name);
            return 0;
        }
        return stored.get().refill(now).tokens();
    }

    /**
     * A CAS over the {@code (tokens, refilled_at)} pair — atomic by construction, like every guarded
     * {@code UPDATE} in the engine, rather than through a specialised lock. The refill and the charge
     * travel in the SAME statement: applying the refill separately would open a window for another node
     * to charge on top of an already refilled balance, which is over-delivery.
     *
     * <p>An internal retry rather than dropping the round on the first collision: a lost CAS costs two
     * round trips, while undoing the round throws away the CAS of up to {@code batchSize} executions.
     * Only after {@link #MAX_CHARGE_ATTEMPTS} is the caller forced to undo.
     */
    @Override
    public boolean charge(String name, int permits, Instant now) {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(now, "now");
        if (permits < 1) {
            throw new IllegalArgumentException("permits must be at least 1, got " + permits);
        }

        for (int attempt = 0; attempt < MAX_CHARGE_ATTEMPTS; attempt++) {
            Optional<Bucket> stored = readBucket(name);
            if (stored.isEmpty()) {
                warnOnceAboutUnknownLimit(name);
                return false;
            }
            Bucket expected = stored.get();
            Bucket refilled = expected.refill(now);
            if (refilled.tokens() < permits) {
                return false;
            }
            if (chargeIfUnchanged(name, expected, refilled, permits)) {
                return true;
            }
        }
        return false;
    }

    /**
     * {@code false} when the row is no longer {@code expected}: another node charged — or a {@code PATCH}
     * changed the spec — between the read and this {@code UPDATE}.
     *
     * <p>The guard covers the FOUR columns that entered the calculation, not only the two it writes:
     * {@code max_count} and {@code window_duration} define the refill interval, and a PATCH widening the
     * window mid-round would refill the balance at the old rate — a silent burst above the new limit.
     */
    private boolean chargeIfUnchanged(String name, Bucket expected, Bucket refilled, int permits) {
        return jdbcTemplate.update(delegate.chargeRateLimitByCas(), new MapSqlParameterSource("name", name)
                .addValue("tokens", refilled.tokens() - permits)
                .addValue("refilledAt", JdbcTimestamps.toUtcLocalDateTime(refilled.refilledAt()))
                .addValue("expectedTokens", expected.tokens())
                .addValue("expectedRefilledAt", JdbcTimestamps.toUtcLocalDateTime(expected.refilledAt()))
                .addValue("expectedMax", expected.max())
                .addValue("expectedWindow", expected.windowText())) == 1;
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
        return new RateLimit(rs.getString("name"), rs.getInt("max_count"), Duration.parse(rs.getString("window_duration")));
    }

    /**
     * The deserialisation boundary is also a trust boundary: {@link RateLimit}'s invariant protects
     * WRITERS (boot, a {@code @Bean}, a PATCH), and the {@code ResultSet} does not go through it.
     *
     * <p>A hand-edited row — a real scenario, since the operator is told to run manual DDL on this table
     * during an upgrade — with {@code max_count} larger than the window in nanoseconds would resurrect
     * the refill's division by zero, taking down the whole claim round. Here that becomes an error that
     * says what to do, and the balance is clamped so one bad row does not blind
     * {@code GET /rate-limits} to the rest.
     */
    private static Bucket mapBucket(ResultSet rs) throws SQLException {
        int max = rs.getInt("max_count");
        Duration window = Duration.parse(rs.getString("window_duration"));
        try {
            RateLimit.requireRefillable(max, window);
        } catch (IllegalArgumentException notRefillable) {
            throw new IllegalStateException("rate limit row (max_count=" + max + ", window_duration=" + window
                    + ") cannot issue tokens — it was not written by Mohs; fix the row", notRefillable);
        }
        // The RAW balance, unclamped: the value read here becomes the CAS's `expectedTokens`, and clamping
        // it in memory would blind the guard — on a row with tokens > max_count the predicate would never
        // match and `charge` would fail forever, taking down every round that touched the limit. What
        // clamps is the dashboard read (`toSnapshot`), which guards nothing.
        //
        // windowText is the row's RAW text, not window.toString(): the CAS compares text, and
        // Duration.parse is lenient while toString is canonical ('PT60S' written by hand comes back as
        // 'PT1M'). Re-serialising here would make the predicate never match on a row edited outside Mohs
        // — the limit would become uncharge­able forever, in silence.
        return new Bucket(max, window, rs.getString("window_duration"), rs.getInt("tokens"),
                JdbcTimestamps.fromUtcLocalDateTime(rs.getObject("refilled_at", LocalDateTime.class)));
    }

    /**
     * The token bucket: one token every {@code window / max}, with a capacity of {@code max}.
     */
    private record Bucket(int max, Duration window, String windowText, int tokens, Instant refilledAt) {

        /**
         * Elapsed time converted into WHOLE tokens, with {@code refilledAt} advancing by what was
         * converted — never to "now". Keeping the pending fraction is what stops the bucket from
         * delivering less than {@code max} per window forever (each call would lose the division's
         * remainder).
         *
         * <p>A clock running backwards gives {@code earned <= 0} and moves nothing: an NTP step into the
         * past delays the release, it never releases twice as much.
         */
        Bucket refill(Instant now) {
            if (tokens > max) {
                // A tampered row: the raw balance remains the CAS's `expected` (what guards is `expected`,
                // not this return value), but what can be SPENT never exceeds the capacity — under-delivering
                // is the safe error, over-delivering is the violation.
                return new Bucket(max, window, windowText, max, now);
            }
            Duration perToken = window.dividedBy(max);
            long earned = Duration.between(refilledAt, now).dividedBy(perToken);
            if (earned <= 0) {
                return this;
            }
            if (earned >= max - tokens) {
                // A full bucket has no pending fraction to preserve — and the shortcut avoids multiplying a
                // duration by an arbitrarily large token count (a row untouched since the previous boot).
                return new Bucket(max, window, windowText, max, now);
            }
            return new Bucket(max, window, windowText, tokens + (int) earned, refilledAt.plus(perToken.multipliedBy(earned)));
        }
    }
}
