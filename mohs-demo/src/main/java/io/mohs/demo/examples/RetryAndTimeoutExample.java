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
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import io.mohs.core.definition.OnDemandJob;
import io.mohs.core.execution.JobContext;
import io.mohs.core.execution.RetryPolicy;

/**
 * <b>Scenario 9 — what happens when the work fails, and when it never ends.</b>
 *
 * <h2>The retry budget</h2>
 *
 * <p>{@code retries} counts attempts BEYOND the first, and its default is <b>1</b>, not 0. That is
 * a deliberate default rather than a timid one: delivery is only at-least-once while there is
 * budget. Reclaiming an execution whose owner died — a node lost, a lease expired, a shutdown that
 * ran out of grace — needs somewhere to reschedule it; with a budget of zero there is nowhere, and
 * the execution becomes a terminal FAILED. That is silently lost work, in exactly the event the
 * product exists to survive.
 *
 * <p>So {@code retries = 0} is a real choice — "at most one invocation, and I accept losing it if a
 * node dies" — and it should be written down deliberately, never inherited by accident.
 *
 * <p>Between attempts the delay is exponential with <b>full jitter</b> (base 1s, capped at 10
 * minutes). Jitter is not decoration: without it, a thousand executions failing on the same outage
 * retry in the same instant and re-create the outage the moment it clears.
 *
 * <h2>The timeout</h2>
 *
 * <p>{@code timeout} bounds ONE attempt. When it fires, {@link JobContext#cancellationRequested()}
 * turns true AND the thread is interrupted — so blocking I/O that honours interruption unblocks,
 * and a computation that does not must poll the flag. Nothing is killed from outside: a handler
 * that ignores both keeps running, and only the cluster-wide Watchdog Bound
 * ({@code mohs.engine.watchdog-timeout}, off by default) will eventually release ownership so a
 * peer can take the work.
 *
 * <p>Pick a timeout above the realistic p99 of the work, not above its average. A timeout set too
 * tight turns a slow day into a retry storm.
 *
 * <h2>When the budget cannot express the rule</h2>
 *
 * <p>{@link RetryPolicy} is a Strategy over the built-in backoff, named by BEAN NAME because the
 * definition is persisted — a name survives a restart, a class name does not survive a rename. A
 * job naming a policy with no matching bean fails the boot instead of silently falling back.
 *
 * <p>The policy REPLACES the budget: while it returns a delay, the execution retries, and
 * {@code retries} becomes information rather than a ceiling. A policy that never returns empty
 * never gives up — which is both the point of writing one and its risk.
 *
 * <p><b>It runs on hot paths.</b> On a handler failure it holds the completion transaction open; on
 * a reclaimed lease it runs on the engine's loop thread, which is the same thread that carries this
 * node's heartbeat — and a node that misses its heartbeat has its work reclaimed by a peer. So: no
 * I/O, no locks, no side effects. It is a decision, and everything it needs to decide with is in
 * {@link RetryPolicy.Failure}.
 */
@Configuration(proxyBeanMethods = false)
public class RetryAndTimeoutExample {

    /**
     * Creates a {@code RetryAndTimeoutExample} instance.
     */
    public RetryAndTimeoutExample() {
    }

    private static final Logger log = LoggerFactory.getLogger(RetryAndTimeoutExample.class);

    /**
     * A policy that distinguishes the two failures that deserve different answers: a request that
     * was wrong will be wrong again, while a timeout may not be.
     *
     * <p>The bean NAME is the contract — {@code @OnDemandJob(retryPolicy = "exampleInvoiceRetries")}
     * on {@link #settleInvoice} refers to this method's name.
     *
     * <ul>
     * <li>{@code IllegalArgumentException} — a bad request: give up immediately, retrying it only
     *     burns capacity.</li>
     * <li>{@code null} error — nobody threw anything; this failure is a lease reclaimed from a dead
     *     node. Wait half a minute and let another node have it.</li>
     * <li>anything else — a flat minute, up to ten attempts, then terminal.</li>
     * </ul>
     */
    @Bean
    RetryPolicy exampleInvoiceRetries() {
        return failure -> switch (failure.error()) {
            case IllegalArgumentException _ -> Optional.empty();
            case null -> Optional.of(Duration.ofSeconds(30));
            default -> failure.failedAttempt() <= 10 ? Optional.of(Duration.ofMinutes(1)) : Optional.empty();
        };
    }

    /**
     * The ordinary case: five attempts in total (one plus four retries), each bounded at two
     * minutes.
     */
    @OnDemandJob(id = "example-retry-budget", retries = 4, timeout = "PT2M")
    void chargeCard() {
        log.info("charging, and willing to try four more times");
    }

    /**
     * The deliberate opposite: one invocation, no retry, loss accepted if the node dies mid-flight.
     * Written down on purpose — this is the one place {@code retries = 0} belongs.
     */
    @OnDemandJob(id = "example-retry-none", retries = 0)
    void notifyOnce() {
        log.info("fire and forget, and honest about it");
    }

    /**
     * The custom policy, referenced by the bean name declared above. {@code retries} is still
     * recorded on the definition and reaches the policy in {@link RetryPolicy.Failure#retries()},
     * but it no longer decides anything — the policy does.
     */
    @OnDemandJob(id = "example-retry-policy", retries = 4, retryPolicy = "exampleInvoiceRetries", timeout = "PT5M")
    void settleInvoice(JobContext ctx) {
        log.info("settling on attempt {}", ctx.attempt());
    }
}
