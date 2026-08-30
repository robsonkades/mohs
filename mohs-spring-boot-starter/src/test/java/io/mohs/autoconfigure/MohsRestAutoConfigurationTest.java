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

import java.util.UUID;

import javax.sql.DataSource;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;

import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.jackson.autoconfigure.JacksonAutoConfiguration;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.type.filter.AnnotationTypeFilter;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.util.ClassUtils;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.DispatcherServlet;

import io.mohs.rest.ActorResolver;
import io.mohs.rest.error.RestExceptionHandler;
import io.mohs.rest.execution.ExecutionsController;
import io.mohs.rest.job.JobsController;
import io.mohs.rest.overview.OverviewController;
import io.mohs.rest.runner.RunnersController;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Closed by default, with a prominent WARN when turned on. Same pattern as
 * {@link MohsAutoConfigurationTest} ({@code ApplicationContextRunner} plus a real H2).
 */
class MohsRestAutoConfigurationTest {

    private final ch.qos.logback.classic.Logger logger =
            (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(MohsRestAutoConfiguration.class);
    private final ListAppender<ILoggingEvent> logAppender = new ListAppender<>();

    @BeforeEach
    void attachLogAppender() {
        logAppender.start();
        logger.addAppender(logAppender);
    }

    @AfterEach
    void detachLogAppender() {
        logger.detachAppender(logAppender);
    }

    private static DataSource freshH2DataSource() {
        JdbcDataSource h2 = new JdbcDataSource();
        h2.setURL("jdbc:h2:mem:mohs-rest-autoconfig-test-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        h2.setUser("sa");
        h2.setPassword("");
        new ResourceDatabasePopulator(new ClassPathResource("schema-h2.sql")).execute(h2);
        return h2;
    }

    private static ApplicationContextRunner runnerWith(DataSource dataSource, String... extraProperties) {
        return new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(MohsAutoConfiguration.class, MohsRestAutoConfiguration.class, JacksonAutoConfiguration.class))
                .withBean(DataSource.class, () -> dataSource)
                .withPropertyValues("mohs.jdbc.dialect=h2")
                .withPropertyValues(extraProperties);
    }

    @Test
    void restControllersAreAbsentByDefault() {
        runnerWith(freshH2DataSource()).run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).doesNotHaveBean(JobsController.class);
            assertThat(context).doesNotHaveBean(ExecutionsController.class);
            assertThat(logAppender.list).isEmpty();
        });
    }

    @Test
    void restControllersAppearWhenEnabledWithAWarnLog() {
        runnerWith(freshH2DataSource(), "mohs.api.enabled=true").run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).hasSingleBean(JobsController.class);
            assertThat(context).hasSingleBean(ExecutionsController.class);
            assertThat(context).hasSingleBean(OverviewController.class);
            assertThat(context).hasSingleBean(RunnersController.class);
            assertThat(context).hasSingleBean(RestExceptionHandler.class);
            assertThat(context).hasSingleBean(ActorResolver.class);
            // Without this bean the SSE stream only closes when the context is destroyed, and the
            // web server's graceful shutdown waits for it until the timeout
            assertThat(context).hasSingleBean(MohsOverviewStreamLifecycle.class);
            assertThat(logAppender.list)
                    .anyMatch(event -> event.getFormattedMessage().contains("NO authentication"));
        });
    }

    /** The master gate wins silently ({@code mohs.enabled}'s documentation: "turning it off removes every Mohs bean") — a kill switch must never become a boot failure. */
    @Test
    void masterGateOffSuppressesTheRestApiEvenWhenExplicitlyEnabled() {
        runnerWith(freshH2DataSource(), "mohs.enabled=false", "mohs.api.enabled=true").run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).doesNotHaveBean(JobsController.class);
            assertThat(context).doesNotHaveBean(ExecutionsController.class);
            assertThat(logAppender.list).isEmpty();
        });
    }

    /**
     * The starter depends on {@code mohs-rest} without {@code <optional>}, so the controllers are
     * always on the consumer's classpath. What carries the weight is
     * {@code @ConditionalOnClass(DispatcherServlet.class)}: with no web stack, Boot reads the
     * auto-configuration's bytecode and backs off BEFORE loading the class, so
     * {@code JobsController} — which references {@code ResponseEntity} and friends — is never
     * resolved.
     *
     * <p>That is the premise behind putting {@code <optional>} in mohs-rest rather than here, and
     * without this test it would be argument only: the other cases in this class exercise the
     * <em>property</em> gate, with {@code DispatcherServlet} present throughout.
     */
    @Test
    void restApiStaysSilentWithoutDispatcherServletInsteadOfFailingTheBoot() {
        runnerWith(freshH2DataSource(), "mohs.api.enabled=true")
                .withClassLoader(new FilteredClassLoader(DispatcherServlet.class))
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).doesNotHaveBean(JobsController.class);
                    assertThat(context).doesNotHaveBean(ExecutionsController.class);
                    assertThat(context).doesNotHaveBean(RestExceptionHandler.class);
                    assertThat(logAppender.list).isEmpty();
                });
    }

    @Test
    void basePathIsConfigurable() {
        runnerWith(freshH2DataSource(), "mohs.api.enabled=true", "mohs.api.base-path=/custom").run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context.getBean(MohsProperties.class).api().basePath()).isEqualTo("/custom");
        });
    }

    /**
     * The defect this prevents: {@code BatchesController} was implemented, contract-tested and
     * documented for a whole milestone while no {@code @Bean} registered it, so the route answered
     * the host's default 404. The contract test could not catch it — a {@code @WebMvcTest} slice
     * builds the controller itself. Only a rule over the whole package can: every
     * {@code @RestController} in {@code io.mohs.rest} is a v1 route, and a v1 route that is not
     * wired does not exist.
     */
    @Test
    void everyRestControllerInThePackageIsRegisteredWhenTheApiIsOn() {
        var scanner = new ClassPathScanningCandidateComponentProvider(false);
        scanner.addIncludeFilter(new AnnotationTypeFilter(RestController.class));
        var declared = scanner.findCandidateComponents("io.mohs.rest").stream()
                .map(BeanDefinition::getBeanClassName)
                .map(name -> ClassUtils.resolveClassName(name, getClass().getClassLoader()))
                .toList();

        assertThat(declared).isNotEmpty();
        runnerWith(freshH2DataSource(), "mohs.api.enabled=true").run(context ->
                assertThat(declared).allSatisfy(controller -> assertThat(context).hasSingleBean(controller)));
    }
}
