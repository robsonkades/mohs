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

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;

import io.mohs.core.resource.RateLimit;
import io.mohs.engine.EngineSettings;

/**
 * A rate limit set to a fraction of demand, a three-node cluster, and an UNLIMITED job competing
 * for the same claim.
 *
 * <p>Two questions, and the second is the one nobody measured: whether the ceiling holds under
 * concurrency (a two-phase CAS), and whether the unlimited job pays for its limited neighbour —
 * because a claim round that fails to close the CAS is undone ENTIRELY, and that round may contain
 * executions of jobs with no limit at all.
 *
 * <p>The ceiling criterion is the token bucket's, not a fixed window's: capacity is {@code max} and
 * refill is one token every {@code window/max}, so the legitimate envelope of the k-th delivery is
 * {@code t_k >= (k - max) x window/max}. Demanding "never more than max in any sliding window"
 * would be demanding a mechanism that was deliberately not chosen.
 *
 * <p>Run by name: {@code ./mvnw -pl mohs-benchmark test -Dtest=RateLimitCeilingScenario}.
 */
class RateLimitCeilingScenario {

    private static final String LIMIT_NAME = "smtp";
    private static final int MAX = 100;
    private static final Duration WINDOW = Duration.ofSeconds(10);
    private static final int NODES = 3;
    private static final int LIMITED_SEED = 1_200;
    private static final int UNLIMITED_SEED = 1_200;
    private static final Duration OBSERVATION = Duration.ofSeconds(45);
    /** The bucket's refill: one token every {@code window/max}. */
    private static final long TOKEN_PERIOD_NANOS = WINDOW.toNanos() / MAX;

    /** The first delivery that broke the bucket's envelope; {@link #NONE} when none did. */
    private record OverDelivery(int k, long aheadNanos) {
        static final OverDelivery NONE = new OverDelivery(-1, -1);
    }

    @Test
    void theCapHoldsAcrossNodesAndTheUnlimitedJobDoesNotPayForIt() {
        DataSource dataSource = PostgresTestSupport.freshSchema();
        Clock clock = Clock.systemUTC();
        List<Long> limitedStarts = Collections.synchronizedList(new ArrayList<>());
        List<Long> unlimitedStarts = Collections.synchronizedList(new ArrayList<>());

        try (ScenarioCluster cluster = new ScenarioCluster(dataSource, clock)) {
            cluster.rateLimits().upsert(new RateLimit(LIMIT_NAME, MAX, WINDOW));
            // retries(0) on both: the bucket ceiling is billed per DELIVERY, and a retry from a
            // reclaim would be one more delivery that did not come from the seed — noise in an
            // assertion that counts deliveries exactly
            cluster.defineJob("limited", spec -> spec.rateLimit(LIMIT_NAME).retries(0));
            cluster.defineJob("unlimited", spec -> spec.retries(0));

            for (int i = 0; i < NODES; i++) {
                cluster.addNode(settings(), List.of());
            }
            cluster.registerEverywhere("limited", (_, _) -> limitedStarts.add(System.nanoTime()));
            cluster.registerEverywhere("unlimited", (_, _) -> unlimitedStarts.add(System.nanoTime()));

            cluster.seedReady("limited", LIMITED_SEED, 20);
            cluster.seedReady("unlimited", UNLIMITED_SEED, 20);

            long startedAt = System.nanoTime();
            cluster.startAll();
            ScenarioCluster.awaitUntil(OBSERVATION, () -> unlimitedStarts.size() >= UNLIMITED_SEED
                    && limitedStarts.size() >= LIMITED_SEED);
            long observedNanos = System.nanoTime() - startedAt;

            // sorted(): reading the clock and adding to the list are not atomic, so with ~192
            // concurrent handlers INSERTION ORDER is not time order — and firstOverDelivery indexes
            // k by position against an envelope that grows with k. Without sorting, an early stamp
            // in a late position invents (or hides) a violation.
            List<Long> limited = limitedStarts.stream().sorted().toList();
            List<Long> unlimited = unlimitedStarts.stream().sorted().toList();
            OverDelivery overDelivery = firstOverDelivery(limited, startedAt);
            long unlimitedDrainNanos = unlimited.size() < UNLIMITED_SEED ? -1
                    : unlimited.getLast() - startedAt;

            report(observedNanos, limited.size(), unlimited.size(), unlimitedDrainNanos, overDelivery);

            // Two proofs, and the aggregate one closes the "smearing" loophole: the token is charged
            // at CLAIM while the stamp comes from the HANDLER, so a round charging 300 tokens at
            // once could spread those 300 executions across the window and still fit the per-k
            // envelope. The total within the window has no such escape.
            assertThat(limited.size())
                    .as("aggregate over-delivery: %d deliveries in %.1fs, the bucket authorises at most %.0f",
                            limited.size(), observedNanos / 1e9, nominalDeliveries(observedNanos))
                    .isLessThanOrEqualTo((int) Math.ceil(nominalDeliveries(observedNanos)));
            assertThat(overDelivery)
                    .as("over-delivery: the %dth limited execution started %.3fs ahead of the token envelope",
                            overDelivery.k(), overDelivery.aheadNanos() / 1e9)
                    .isEqualTo(OverDelivery.NONE);
            assertThat(unlimited)
                    .as("the unlimited job must drain completely — it shares the claim rounds with a saturated limit")
                    .hasSize(UNLIMITED_SEED);
            assertThat(Duration.ofNanos(unlimitedDrainNanos))
                    .as("collateral damage: the unlimited job drained as slowly as the limited one")
                    .isLessThan(Duration.ofSeconds(20));

            assertThat(limited.size() / nominalDeliveries(observedNanos))
                    .as("under-delivery beyond the expected burn (the kill line is 90%% of nominal)")
                    .isGreaterThan(0.90);
        }
    }

    /**
     * Scans the limited job's deliveries for the first one that started EARLIER than the bucket
     * would allow: the k-th is only legitimate from {@code (k - MAX) x TOKEN_PERIOD_NANOS} after
     * start-up, because the first MAX fit in the burst of a full bucket.
     */
    private static OverDelivery firstOverDelivery(List<Long> limitedStarts, long startedAt) {
        for (int k = MAX + 1; k <= limitedStarts.size(); k++) {
            long elapsed = limitedStarts.get(k - 1) - startedAt;
            long envelope = (long) (k - MAX) * TOKEN_PERIOD_NANOS;
            if (elapsed < envelope) {
                return new OverDelivery(k, envelope - elapsed);
            }
        }
        return OverDelivery.NONE;
    }

    /** Quantas entregas o bucket autoriza na janela observada: a capacidade inicial mais o refill. */
    private static double nominalDeliveries(long observedNanos) {
        return (double) observedNanos / TOKEN_PERIOD_NANOS + MAX;
    }

    private static EngineSettings settings() {
        return new EngineSettings(Duration.ofMillis(25), Duration.ofMillis(200), 64, 64, 3,
                Duration.ofSeconds(30), Duration.ofSeconds(15), null, Duration.ofSeconds(60));
    }

    private static void report(long observedNanos, int limited, int unlimited, long unlimitedDrainNanos,
            OverDelivery overDelivery) {
        System.out.printf("""

                === S7 — rate limit ceiling (%d nodes, max=%d per %s) ===
                observation window   : %.1fs
                limited delivered    : %d of %d seeded
                unlimited delivered  : %d of %d seeded
                unlimited full drain : %s
                nominal for window   : %.0f (capacity %d + refill)
                over-delivery        : %s
                """, NODES, MAX, WINDOW, observedNanos / 1e9, limited, LIMITED_SEED, unlimited, UNLIMITED_SEED,
                unlimitedDrainNanos < 0 ? "NOT REACHED" : "%.1fs".formatted(unlimitedDrainNanos / 1e9),
                nominalDeliveries(observedNanos), MAX,
                overDelivery.equals(OverDelivery.NONE) ? "none"
                        : "at k=%d, %.3fs early".formatted(overDelivery.k(), overDelivery.aheadNanos() / 1e9));
    }
}
