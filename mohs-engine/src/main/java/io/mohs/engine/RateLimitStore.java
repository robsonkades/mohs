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
package io.mohs.engine;

import java.time.Instant;
import java.util.Optional;
import java.util.stream.Stream;

import io.mohs.core.RateLimitSnapshot;
import io.mohs.core.resource.RateLimit;

/**
 * Persistence of a {@link RateLimit} — a Repository (PoEAA), the port {@code io.mohs.store.jdbc}
 * implements.
 *
 * <p>It also holds the enforcement's operational state: the token bucket (its balance plus the
 * instant up to which time has already become tokens), read by {@link #available} and charged by
 * {@link #charge} across the claim's two phases.
 */
public interface RateLimitStore {

    /**
     * Writes the spec — and only the spec. The bucket is operational state and survives the upsert
     * (its tokens clamped to the new {@code max}): resetting here would make every node coming up in
     * a rolling deploy hand back a full bucket, turning a deploy into a burst. The same reasoning as
     * {@code paused} in {@link JobStore#upsert}: boot configuration governs the spec, never the
     * current state.
     */
    RateLimit upsert(RateLimit rateLimit);

    /**
     * PHASE 1 of the two-phase consumption: how many tokens the bucket would grant right now. A pure
     * read — no lock, no write, and therefore no serialisation cost: it is what allows deciding the
     * batch's admission without holding the row for the whole claim.
     *
     * <p>A nonexistent limit returns 0 (fail-safe): a job pointing at a name that does not exist stops
     * running rather than running without the limit somebody asked for — the same stance as
     * {@link ExecutionWindowRegistry#excludes} for an unknown window.
     *
     * @param now the instant from the caller's injected {@code Clock}; the refill is computed against
     *        it, never against the database's clock
     */
    int available(String name, Instant now);

    /**
     * PHASE 2: charges exactly {@code permits} from the bucket, all or nothing.
     *
     * <p>Called at the END of the claim transaction, after the CAS — it charges what was CLAIMED, not
     * what was admitted, so a token does not burn on a candidate that lost the job's mutex. The row
     * lock is born here and dies at the commit: the serialisation window becomes the transaction's
     * tail rather than the whole transaction (measured: 2.3x at 4 clients, 3.5x at 8).
     *
     * <p>Atomicity comes from a guarded {@code UPDATE}, not a specialised lock. It requires READ
     * COMMITTED isolation: the implementation re-reads the row between CAS attempts, and under
     * REPEATABLE READ the re-read would return the same snapshot — the attempts would fail
     * identically and the retry would become an expensive no-op. {@code JdbcWorkQueue} guarantees
     * this explicitly; another caller inheriting a {@code @Transactional} transaction from the host
     * must guarantee the same.
     *
     * <p>{@code false} means another node changed the bucket between the two phases and there is no
     * balance left: the caller MUST undo the round, because the executions have already been claimed
     * and delivering them without a token would be over-delivery, the one unacceptable violation of
     * the contract.
     */
    boolean charge(String name, int permits, Instant now);

    /** The spec plus the bucket's current balance, with the refill applied in memory — a pure read that neither writes nor consumes. */
    Optional<RateLimitSnapshot> find(String name);

    /**
     * A stream over an open cursor — the caller owns the lifecycle (try-with-resources). On Postgres
     * this only holds inside a transaction (autocommit off) — outside one, the driver materialises
     * the entire result before returning the first item.
     */
    Stream<RateLimitSnapshot> findAll();
}
