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
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.jackson.autoconfigure.JacksonAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

import io.mohs.rest.ActorResolver;
import io.mohs.rest.error.RestExceptionHandler;
import io.mohs.rest.execution.ExecutionsController;
import io.mohs.rest.job.JobsController;
import io.mohs.rest.overview.OverviewController;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * ADR-0010, princípio 5: fechada por padrão, WARN destacado quando ligada.
 * Mesmo padrão de {@link MohsAutoConfigurationTest} ({@code
 * ApplicationContextRunner} + H2 real).
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
            assertThat(context).hasSingleBean(RestExceptionHandler.class);
            assertThat(context).hasSingleBean(ActorResolver.class);
            assertThat(logAppender.list)
                    .anyMatch(event -> event.getFormattedMessage().contains("sem autenticação"));
        });
    }

    /** Gate mestre vence em silêncio (Javadoc de {@code mohs.enabled}: "desligar remove todos os beans do Mohs") — kill switch nunca pode virar falha de boot. */
    @Test
    void masterGateOffSuppressesTheRestApiEvenWhenExplicitlyEnabled() {
        runnerWith(freshH2DataSource(), "mohs.enabled=false", "mohs.api.enabled=true").run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).doesNotHaveBean(JobsController.class);
            assertThat(context).doesNotHaveBean(ExecutionsController.class);
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
}
