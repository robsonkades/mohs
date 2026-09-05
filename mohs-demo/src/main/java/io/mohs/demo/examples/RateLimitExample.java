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
package io.mohs.demo.examples;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import io.mohs.core.Mohs;
import io.mohs.core.RateLimitSnapshot;
import io.mohs.core.definition.OnDemandJob;
import io.mohs.core.resource.RateLimit;

/**
 * <b>Scenario 12 — a cluster-wide cap on how often work starts.</b>
 *
 * <p>The SMTP relay accepts 100 messages a minute. Not 100 per node, not 100 per process: 100,
 * across the whole cluster, however many replicas are running today. That is what a rate limit is,
 * and it is the axis a {@link RunnerExample runner} cannot express — a runner bounds how much runs
 * at once ON THIS NODE, while a rate limit bounds how often work may start ANYWHERE.
 *
 * <table border="1">
 * <caption>Three different bounds, often confused</caption>
 * <tr><th>Mechanism</th><th>Bounds</th><th>Scope</th></tr>
 * <tr><td>{@link RunnerExample Runner}</td><td>concurrency</td><td>this node</td></tr>
 * <tr><td>{@link ConcurrencyPolicyExample Overlap policy}</td><td>concurrency of ONE job</td><td>the cluster</td></tr>
 * <tr><td>Rate limit</td><td>THROUGHPUT — starts per window</td><td>the cluster</td></tr>
 * </table>
 *
 * <p>The mechanism is a token bucket held in the database, refilled at {@code window / max}. Nothing
 * is discarded when the bucket is empty: the claim simply passes the job over and takes it on a
 * later round, so the work is delayed rather than dropped.
 *
 * <p>Referencing a limit that does not exist blocks the job on purpose — running without the cap
 * somebody explicitly asked for is worse than not running at all.
 *
 * <h2>Declaring one</h2>
 *
 * <p>A bean (below) defines the structure; a property adjusts the numbers, and wins when both are
 * present, because a property can be changed without recompiling:
 *
 * {@snippet lang="yaml" :
 * mohs:
 *   rate-limits:
 *     example-smtp:
 *       max: 100
 *       window: 1m
 *}
 *
 * <p>A limit is SHARED state, so declaring it at boot writes to the database, and drift against what
 * is stored follows {@code mohs.registration.on-conflict} exactly like a job definition. The bucket
 * balance itself is never touched by boot: boot governs the spec, not the current state.
 */
@Configuration(proxyBeanMethods = false)
public class RateLimitExample {

    private static final Logger log = LoggerFactory.getLogger(RateLimitExample.class);

    private final Mohs mohs;

    /**
     * Creates a {@code RateLimitExample} with the supplied values.
     *
     * @param mohs the scheduling and operations facade
     */
    public RateLimitExample(Mohs mohs) {
        this.mohs = mohs;
    }

    /** 100 starts per minute, cluster-wide — one token issued every 600 ms. */
    @Bean
    RateLimit exampleSmtpRateLimit() {
        return new RateLimit("example-smtp", 100, Duration.ofMinutes(1));
    }

    /**
     * The emergency knob: change {@code max}/{@code window} of an already declared limit at runtime,
     * cluster-wide. The bucket survives the change with its balance clamped to the new ceiling —
     * lowering the limit cuts future throughput, it does not claw back what was already spent.
     *
     * <p>It is a PATCH, not a redefinition: the next boot reapplies the code's value under the
     * default {@code on-conflict=override}. Declaring a NEW limit is an act of boot, so an unknown
     * name returns empty rather than creating one.
     *
     * @param max the maximum permitted count
     * @param window the positive window over which the rate is limited
     * @return the updated SMTP rate limit, or empty when absent
     */
    public Optional<RateLimitSnapshot> throttleSmtpTo(int max, Duration window) {
        return mohs.adjustRateLimit("example-smtp", max, window);
    }

    /**
     * The declared limits with each bucket's current balance. A pure read — checking the balance
     * consumes no token — which is what makes it safe on a dashboard poll.
     *
     * @return the declared rate limits and their current balances
     */
    public List<RateLimitSnapshot> currentBalances() {
        return mohs.rateLimits();
    }

    @OnDemandJob(id = "example-send-newsletter", rateLimit = "example-smtp", retries = 3)
    void sendNewsletter() {
        log.info("one of at most a hundred a minute, across the whole cluster");
    }
}
