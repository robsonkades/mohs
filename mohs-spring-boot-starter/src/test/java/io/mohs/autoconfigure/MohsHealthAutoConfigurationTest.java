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

import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

import static org.assertj.core.api.Assertions.assertThat;

/** Same shape as {@link MohsRestAutoConfigurationTest}: an {@code ApplicationContextRunner} over a real H2. */
class MohsHealthAutoConfigurationTest {

    @Test
    void delayedStartupIsOutOfServiceWithStartingDetail() {
        runnerWith(freshH2DataSource(), "mohs.lifecycle.startup-delay=1h").run(context -> {
            assertThat(context).hasNotFailed();
            var health = context.getBean(MohsHealthIndicator.class).health();
            assertThat(health.getStatus().getCode()).isEqualTo("OUT_OF_SERVICE");
            assertThat(health.getDetails()).containsEntry("state", "STARTING");
        });
    }

    private static DataSource freshH2DataSource() {
        JdbcDataSource h2 = new JdbcDataSource();
        h2.setURL("jdbc:h2:mem:mohs-health-autoconfig-test-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        h2.setUser("sa");
        h2.setPassword("");
        new ResourceDatabasePopulator(new ClassPathResource("schema-h2.sql")).execute(h2);
        return h2;
    }

    private static ApplicationContextRunner runnerWith(DataSource dataSource, String... extraProperties) {
        return new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(MohsAutoConfiguration.class, MohsHealthAutoConfiguration.class))
                .withBean(DataSource.class, () -> dataSource)
                .withPropertyValues("mohs.jdbc.dialect=h2")
                .withPropertyValues(extraProperties);
    }

    /** The bean name is the key the indicator appears under in {@code /actuator/health} — {@code mohs}, not a class name. */
    @Test
    void theIndicatorIsContributedWhenTheActuatorIsOnTheClasspath() {
        runnerWith(freshH2DataSource()).run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).hasSingleBean(MohsHealthIndicator.class);
            assertThat(context).hasBean("mohsHealthIndicator");
        });
    }

    /**
     * {@code spring-boot-health} is {@code optional}, so the common case is an application that
     * never brought it. Boot must back off from the auto-configuration's bytecode — a failure here
     * would mean every Mohs consumer without an actuator gets a {@code ClassNotFoundException}.
     */
    @Test
    void nothingIsContributedWithoutTheActuatorInsteadOfFailingTheBoot() {
        runnerWith(freshH2DataSource())
                .withClassLoader(new FilteredClassLoader(HealthIndicator.class))
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).doesNotHaveBean(MohsHealthIndicator.class);
                });
    }

    /** The master gate removes every Mohs bean — the health entry included. */
    @Test
    void masterGateOffSuppressesTheIndicator() {
        runnerWith(freshH2DataSource(), "mohs.enabled=false").run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).doesNotHaveBean(MohsHealthIndicator.class);
        });
    }

    /** The standard actuator switch, for a host that would rather answer the question its own way. */
    @Test
    void theIndicatorCanBeDisabledTheStandardWay() {
        runnerWith(freshH2DataSource(), "management.health.mohs.enabled=false").run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).doesNotHaveBean(MohsHealthIndicator.class);
        });
    }
}
