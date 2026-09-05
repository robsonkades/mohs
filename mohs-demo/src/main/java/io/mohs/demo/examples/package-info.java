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
/**
 * A worked example of every way Mohs can be used — one class per scenario, all of them real beans
 * of this application, so the catalogue is compiled and booted rather than merely written down.
 *
 * <p>Everything here is API a consumer has: the {@code io.mohs.core} vocabulary plus the starter's
 * properties. Nothing reaches into {@code io.mohs.engine} or {@code io.mohs.store.jdbc} — the same
 * boundary {@code ArchitectureTest} enforces on this package.
 *
 * <h2>Reading order</h2>
 *
 * <p><b>Defining a job</b>
 * <ol>
 * <li>{@link io.mohs.demo.examples.CronJobExample} — a schedule in cron, always with an explicit zone</li>
 * <li>{@link io.mohs.demo.examples.IntervalJobExample} — fixed rate versus fixed delay</li>
 * <li>{@link io.mohs.demo.examples.OnDemandJobExample} — no schedule: a typed payload, invoked by the application</li>
 * <li>{@link io.mohs.demo.examples.HandlerSignatureExample} — the four handler signatures, and what {@code JobContext} gives you</li>
 * <li>{@link io.mohs.demo.examples.ComposedStereotypeExample} — your own annotation over {@code @MohsJob}</li>
 * <li>{@link io.mohs.demo.examples.ProgrammaticDefinitionExample} — data-driven definitions, without annotations</li>
 * </ol>
 *
 * <p><b>Invoking it</b>
 * <ol start="7">
 * <li>{@link io.mohs.demo.examples.SchedulingOptionsExample} — {@code now}/{@code at}/{@code after}, priority, actor, idempotency key</li>
 * <li>{@link io.mohs.demo.examples.BatchExample} — many jobs as one unit, with a completion callback</li>
 * </ol>
 *
 * <p><b>Policy: what happens when it runs</b>
 * <ol start="9">
 * <li>{@link io.mohs.demo.examples.RetryAndTimeoutExample} — the retry budget, a per-attempt timeout, a custom policy</li>
 * <li>{@link io.mohs.demo.examples.ConcurrencyPolicyExample} — overlap between executions of the same job</li>
 * <li>{@link io.mohs.demo.examples.RunnerExample} — named runners, IO (virtual threads) and CPU (bounded pool)</li>
 * <li>{@link io.mohs.demo.examples.RateLimitExample} — a cluster-wide throughput cap over a shared resource</li>
 * <li>{@link io.mohs.demo.examples.ExecutionWindowExample} — firing times that are excluded outright</li>
 * </ol>
 *
 * <p><b>Observing and operating it</b>
 * <ol start="14">
 * <li>{@link io.mohs.demo.examples.EventListenerExample} — {@code ExecutionListener} and {@code @OnExecution}</li>
 * <li>{@link io.mohs.demo.examples.InterceptorExample} — {@code ExecutionInterceptor}, on the attempt's own thread</li>
 * <li>{@link io.mohs.demo.examples.OperationsExample} — pause, reschedule, cancel, retry, query, drain</li>
 * </ol>
 *
 * <h2>The configuration these examples run on</h2>
 *
 * <p>An application needs exactly two things to have Mohs: the starter on the classpath and a
 * {@code DataSource}. Everything below is optional and every value shown is the default, with three
 * exceptions marked in the snippet itself: {@code jdbc.dialect} has no default and is mandatory,
 * and the {@code runners}/{@code rate-limits} entries at the end are illustrative — nothing named
 * {@code reports} or {@code smtp} exists until you declare it.
 *
 * {@snippet lang="yaml" :
 * mohs:
 *   enabled: true                      # false removes the whole auto-configuration
 *   jdbc:
 *     dialect: postgresql              # h2 | postgresql | mysql | sqlserver — MANDATORY, never auto-detected
 *   registration:
 *     on-conflict: override            # what a redeploy does when the code and the store disagree:
 *                                      # override (code wins) | preserve (store wins) | fail (boot stops)
 *   time:
 *     mode: application                # application (system clock, hosts on NTP) | database (sampled offset)
 *   lifecycle:
 *     start-mode: auto                 # manual leaves the engine stopped until Mohs.lifecycle().start()
 *     shutdown:
 *       grace-period: 30s              # how long a drain waits for in-flight work
 *   engine:
 *     poll-interval: 25ms              # the floor of the adaptive poll
 *     max-poll-interval: 2s            # the ceiling it backs off to when idle
 *     batch-size: 50                   # candidates per claim round
 *     dispatch-concurrency: 64         # this node's in-flight ceiling; also the default 'io' runner's size
 *     lease-ttl: 30s                   # an execution's ownership lease
 *     node-lease-ttl: 15s              # the node's own heartbeat lease (12s floor, validated at boot)
 *     misfire-threshold: 60s           # later than this, a firing counts as MISSED rather than late
 *     watchdog-timeout:                # unset: no cluster-wide bound on how long one attempt may run
 *   api:
 *     enabled: false                   # the operational REST API, off by default
 *     base-path: /api/mohs/v1
 *   runners:                           # ILLUSTRATIVE, not a default: the property form of RunnerExample's beans
 *     reports:
 *       mode: io
 *       max: 32
 *   rate-limits:                       # ILLUSTRATIVE, not a default: the property form of RateLimitExample's bean
 *     smtp:
 *       max: 100
 *       window: 1m
 *}
 *
 * <p>Three things deserve a sentence each, because getting them wrong is silent:
 *
 * <ul>
 * <li><b>The schema is yours to install.</b> Mohs runs no DDL — apply {@code schema-<dialect>.sql}
 *     from {@code mohs-store-jdbc}'s jar before the application starts, and the {@code V*.sql}
 *     deltas when you upgrade. Skip it and the first write fails at boot with the driver's own
 *     "table does not exist". This demo installs it through {@code spring.sql.init}, which is the
 *     shape a host copies for a dev profile.</li>
 * <li><b>{@code mohs.jdbc.dialect} is mandatory.</b> It is never inferred from the
 *     {@code DataSource} — a guess that is right in development and wrong in production is worse
 *     than a boot failure. {@code h2} logs a WARN: it is a development tier, not a supported
 *     production backend.</li>
 * <li><b>{@code mohs.api.enabled} is off by default.</b> The dashboard at {@code /mohs-ui} (the
 *     separate {@code mohs-ui} dependency) consumes that API, so a dashboard on a host that never
 *     enabled the API shows nothing.</li>
 * </ul>
 */
@NullMarked
package io.mohs.demo.examples;

import org.jspecify.annotations.NullMarked;
