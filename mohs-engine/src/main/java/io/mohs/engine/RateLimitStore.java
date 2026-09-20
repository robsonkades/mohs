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

import java.util.Optional;
import java.util.stream.Stream;
import io.mohs.core.RateLimitSnapshot;
import io.mohs.core.resource.RateLimit;

/** Shared token buckets. Their own authoritative clock decides refill; callers cannot advance it. */
public interface RateLimitStore {
    /**
     * Creates a full bucket or changes its specification while retaining the current balance,
     * clamped to the new capacity. Identical configuration preserves fractional refill credit.
     * A change resets fractional credit at the store's decision time. Concurrent configuration
     * and charges must not overwrite each other's state.
     * JDBC configuration commits in its own bounded READ_COMMITTED transaction, independently
     * of any host transaction. An unknown commit can be reconciled by repeating the desired spec.
     */
    RateLimit upsert(RateLimit rateLimit);

    /**
     * Advisory available whole tokens, with refill evaluated on the store's clock.
     * This read consumes nothing. A missing limit returns zero. A later charge can lose a race.
     */
    int available(String name);

    /**
     * Atomically consumes exactly permits, or none. JDBC uses a fresh database-time sample and
     * a CAS over capacity, window, balance, refill instant and fractional credit, with bounded
     * retries in an independent READ_COMMITTED transaction. This operation occurs after claim,
     * outside its transaction. The caller dispatches only the number successfully charged.
     * Tokens are not refunded if dispatch or the process subsequently fails. An unknown result
     * must not be treated as authorization to dispatch, or repeated as if it had rolled back.
     * @param name declared resource name
     * @param permits positive requested number of permits
     * @return whether all permits were durably charged
     */
    boolean charge(String name, int permits);

    /** Read-only specification and currently refilled balance; empty for an unknown name. */
    Optional<RateLimitSnapshot> find(String name);

    /** Open cursor of read-only snapshots. The caller closes it to release its connection. */
    Stream<RateLimitSnapshot> findAll();
}
