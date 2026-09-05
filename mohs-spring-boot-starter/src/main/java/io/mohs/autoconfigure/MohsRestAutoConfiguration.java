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
package io.mohs.autoconfigure;

import java.time.Clock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.web.servlet.DispatcherServlet;

import io.mohs.core.Mohs;
import io.mohs.rest.ActorResolver;
import io.mohs.rest.HeaderActorResolver;
import io.mohs.rest.batch.BatchesController;
import io.mohs.rest.error.RestExceptionHandler;
import io.mohs.rest.execution.ExecutionsController;
import io.mohs.rest.job.JobsController;
import io.mohs.rest.node.NodesController;
import io.mohs.rest.overview.OverviewController;
import io.mohs.rest.overview.OverviewStreamBroadcaster;
import io.mohs.rest.ratelimit.RateLimitsController;
import io.mohs.rest.runner.RunnersController;

/**
 * Wires the v1 REST contract ({@code io.mohs.rest}) to the public {@link Mohs} facade — closed by
 * default: {@code mohs.api.enabled=false} registers no bean from that package.
 * {@link ConditionalOnClass} on {@link DispatcherServlet} avoids loading this configuration when
 * the consumer did not bring {@code spring-boot-starter-webmvc} (an {@code optional} dependency of
 * the module, the same pattern as the actuator).
 *
 * <p>It is also conditional on the master {@code mohs.enabled} gate: the kill switch wins silently.
 * The gate's own documentation promises that turning it off removes every Mohs bean, and a boot
 * failure here would turn the emergency button into a crash whenever {@code mohs.api.enabled=true}
 * was already set in the environment. Without that condition, the combination fell into a generic
 * {@code NoSuchBeanDefinitionException} while creating {@link JobsController} (there is no
 * {@link Mohs} bean).
 *
 * <p>The protection covers only the property gate: a host that excludes
 * {@link MohsAutoConfiguration} by hand ({@code spring.autoconfigure.exclude}) with the API on
 * keeps the boot error — manually excluding the library's own auto-configuration is deliberately
 * unsupported (the alternative, {@code @ConditionalOnBean(Mohs.class)}, would also hide genuine
 * misconfiguration that ought to blow up).
 *
 * <p>{@link ActorResolver} is {@link ConditionalOnMissingBean}: 1.x can swap
 * {@link HeaderActorResolver} (declarative attribution, not authenticated) for a real security
 * implementation without changing any contract.
 */
@AutoConfiguration(after = MohsAutoConfiguration.class)
@ConditionalOnProperty(prefix = "mohs", name = "enabled", matchIfMissing = true)
@ConditionalOnProperty(prefix = "mohs.api", name = "enabled", havingValue = "true")
@ConditionalOnClass(DispatcherServlet.class)
public class MohsRestAutoConfiguration {

    private static final Logger log = LoggerFactory.getLogger(MohsRestAutoConfiguration.class);

    /**
     * The only active guardrail a user reads before exposing the API, which is why its wording
     * matters more than an ordinary log line's: it says what the API CAN do, and what to do about
     * it — a warning that names the risk without naming the remedy only tells an operator to worry.
     *
     * @param properties the bound Mohs configuration properties
     */
    public MohsRestAutoConfiguration(MohsProperties properties) {
        String basePath = properties.api().basePath();
        // The value is concatenated into every route and into the Location of every 202 receipt: an
        // empty prefix mounts the API at the host's root, outside the securityMatcher the docs
        // recommend, and a stray slash builds a Location that points at a 404 — the one outcome a
        // receipt must never produce, because the client resends
        if (!basePath.startsWith("/") || basePath.endsWith("/")) {
            throw new IllegalStateException("mohs.api.base-path must start with '/' and must not end with one, got '"
                    + basePath + "' — it is the prefix of every route and of the Location header on every 202");
        }
        log.warn("mohs.api.enabled=true: the operational API is served at {} with NO authentication. It can trigger"
                + " any registered job with a caller-supplied payload, cancel and retry executions, pause, resume"
                + " and reschedule jobs, and change rate limits. Restrict it to an internal network, or put a"
                + " gateway/mTLS in front of {} and /mohs-ui before exposing this instance.",
                basePath, basePath);
    }

    /**
     * Creates the {@code ActorResolver} bean for Mohs integration.
     *
     * @return the configured {@code ActorResolver} bean
     */
    @Bean
    @ConditionalOnMissingBean(ActorResolver.class)
    public ActorResolver mohsActorResolver() {
        return new HeaderActorResolver();
    }

    /**
     * Creates the {@code RestExceptionHandler} bean for Mohs integration.
     *
     * @return the configured {@code RestExceptionHandler} bean
     */
    @Bean
    public RestExceptionHandler mohsRestExceptionHandler() {
        return new RestExceptionHandler();
    }

    /**
     * Creates the {@code JobsController} bean for Mohs integration.
     *
     * @param mohs the scheduling and operations facade
     * @param mohsActorResolver the resolver that attributes HTTP operations to an actor
     * @param properties the bound Mohs configuration properties
     * @return the configured {@code JobsController} bean
     */
    @Bean
    public JobsController mohsJobsController(Mohs mohs, ActorResolver mohsActorResolver, MohsProperties properties) {
        // The request body is converted with Mohs' own mapper, the one the store reads with — never
        // the context's ObjectMapper — in its strict variant (PayloadMapper says why for both)
        return new JobsController(mohs, mohsActorResolver, PayloadMapper.STRICT, properties.api().basePath());
    }

    /**
     * Creates the {@code ExecutionsController} bean for Mohs integration.
     *
     * @param mohs the scheduling and operations facade
     * @param mohsActorResolver the resolver that attributes HTTP operations to an actor
     * @return the configured {@code ExecutionsController} bean
     */
    @Bean
    public ExecutionsController mohsExecutionsController(Mohs mohs, ActorResolver mohsActorResolver) {
        return new ExecutionsController(mohs, mohsActorResolver);
    }

    /**
     * Creates the {@code BatchesController} bean for Mohs integration.
     *
     * @param mohs the scheduling and operations facade
     * @return the configured {@code BatchesController} bean
     */
    @Bean
    public BatchesController mohsBatchesController(Mohs mohs) {
        return new BatchesController(mohs);
    }

    /**
     * Creates the {@code NodesController} bean for Mohs integration.
     *
     * @param mohs the scheduling and operations facade
     * @return the configured {@code NodesController} bean
     */
    @Bean
    public NodesController mohsNodesController(Mohs mohs) {
        return new NodesController(mohs);
    }

    /**
     * Node-local by nature: it describes the process answering the request, not the cluster (see {@link io.mohs.core.RunnerSnapshot}).
     *
     * @param mohs the scheduling and operations facade
     * @return the configured {@code RunnersController} bean
     */
    @Bean
    public RunnersController mohsRunnersController(Mohs mohs) {
        return new RunnersController(mohs);
    }

    /**
     * Creates the {@code RateLimitsController} bean for Mohs integration.
     *
     * @param mohs the scheduling and operations facade
     * @param mohsActorResolver the resolver that attributes HTTP operations to an actor
     * @return the configured {@code RateLimitsController} bean
     */
    @Bean
    public RateLimitsController mohsRateLimitsController(Mohs mohs, ActorResolver mohsActorResolver) {
        return new RateLimitsController(mohs, mohsActorResolver);
    }

    /**
     * {@code AutoCloseable}: the bean's destroy method is the backstop — what closes the streams within the deadline that matters is {@link MohsOverviewStreamLifecycle}.
     *
     * @param mohs the scheduling and operations facade
     * @param mohsClock the time source used by the component
     * @return the configured {@code OverviewStreamBroadcaster} bean
     */
    @Bean
    public OverviewStreamBroadcaster mohsOverviewStreamBroadcaster(Mohs mohs, @Qualifier("mohsClock") Clock mohsClock) {
        return OverviewStreamBroadcaster.start(mohs, mohsClock);
    }

    /**
     * {@link SmartLifecycle} — closes the SSE streams before the web server starts waiting on active requests; without it shutdown spends the whole phase (see {@link MohsOverviewStreamLifecycle}'s Javadoc).
     *
     * @param mohsOverviewStreamBroadcaster the shared overview event broadcaster
     * @return the configured {@code SmartLifecycle} bean
     */
    @Bean
    public SmartLifecycle mohsOverviewStreamLifecycle(OverviewStreamBroadcaster mohsOverviewStreamBroadcaster) {
        return new MohsOverviewStreamLifecycle(mohsOverviewStreamBroadcaster);
    }

    /**
     * Creates the {@code OverviewController} bean for Mohs integration.
     *
     * @param mohs the scheduling and operations facade
     * @param mohsOverviewStreamBroadcaster the shared overview event broadcaster
     * @return the configured {@code OverviewController} bean
     */
    @Bean
    public OverviewController mohsOverviewController(Mohs mohs, OverviewStreamBroadcaster mohsOverviewStreamBroadcaster) {
        return new OverviewController(mohs, mohsOverviewStreamBroadcaster);
    }
}
