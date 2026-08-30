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

import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.runner.WebApplicationContextRunner;
import org.springframework.core.io.ClassPathResource;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.DispatcherServlet;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * This auto-configuration's gate is the bundle on the classpath rather than a class:
 * {@code mohs-ui} is a resource-only jar.
 *
 * <p><b>Why a stub exists in {@code src/test/resources/mohs-ui-webapp/}.</b> This module does not
 * depend on {@code mohs-ui}, so {@code classpath:/mohs-ui-webapp/index.html} previously never
 * existed in tests — {@code @ConditionalOnResource} blocked everything and the other two gates were
 * NEVER exercised. Deleting {@code @ConditionalOnProperty} from production kept this class green,
 * and the {@code @ConditionalOnClass} test described a {@code NoClassDefFoundError} that could
 * never happen in the scenario as assembled. With the bundle present, all three gates become real.
 */
class MohsUiAutoConfigurationTest {

    private final WebApplicationContextRunner runner = new WebApplicationContextRunner()
            .withUserConfiguration(PropertiesConfiguration.class)
            .withConfiguration(AutoConfigurations.of(MohsUiAutoConfiguration.class));

    /** With the bundle, a web stack and the master gate on, the dashboard is served. This is the baseline for the other three. */
    @Test
    void dashboardIsServedWhenTheBundleIsOnTheClasspath() {
        runner.run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).hasBean("mohsUiStaticAppConfigurer");
        });
    }

    /** Without the bundle nothing activates — and the boot of someone who does not want a dashboard does not break because of it. */
    @Test
    void dashboardIsAbsentWhenTheBundleIsNotOnTheClasspath() {
        runner.withClassLoader(new FilteredClassLoader(new ClassPathResource("mohs-ui-webapp/index.html")))
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).doesNotHaveBean("mohsUiStaticAppConfigurer");
                });
    }

    /**
     * Even with the bundle present, an application without a web stack must not break: the
     * auto-configuration references {@link WebMvcConfigurer} and {@code PathResourceResolver} in
     * its body, and it is {@code @ConditionalOnClass(DispatcherServlet.class)} that stops Spring
     * from even loading the class. Without that gate it would be a
     * {@code NoClassDefFoundError} at boot, not one bean fewer — and the scenario now genuinely
     * exercises it, because the bundle is there.
     */
    @Test
    void dashboardStaysSilentWithoutDispatcherServletInsteadOfFailingTheBoot() {
        runner.withClassLoader(new FilteredClassLoader(DispatcherServlet.class)).run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).doesNotHaveBean(WebMvcConfigurer.class);
        });
    }

    /** Kill switch mestre: {@code mohs.enabled=false} remove todos os beans do Mohs, o dashboard junto. */
    @Test
    void masterGateOffSuppressesTheDashboard() {
        runner.withPropertyValues("mohs.enabled=false").run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).doesNotHaveBean("mohsUiStaticAppConfigurer");
        });
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(MohsProperties.class)
    static class PropertiesConfiguration {
    }
}
