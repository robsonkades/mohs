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

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.health.autoconfigure.contributor.ConditionalOnEnabledHealthIndicator;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.context.annotation.Bean;

import io.mohs.core.Mohs;

/**
 * Contributes {@link MohsHealthIndicator} to {@code /actuator/health} when the host application
 * brought the actuator.
 *
 * <p>{@code spring-boot-health} is an {@code optional} dependency of the starter — the same pattern
 * as the web stack behind {@link MohsRestAutoConfiguration}. An application with no actuator does
 * not inherit one, and {@link ConditionalOnClass} makes Boot back off from this configuration's
 * bytecode before {@link MohsHealthIndicator} — which references {@code Health} — is ever loaded.
 *
 * <p>The bean is named {@code mohsHealthIndicator}, so the entry appears under {@code mohs} in the
 * health response, and it is disableable the standard way
 * ({@code management.health.mohs.enabled=false}). {@link ConditionalOnMissingBean} lets a host that
 * wants a different mapping — or one that also checks its own store — replace it outright.
 */
@AutoConfiguration(after = MohsAutoConfiguration.class)
@ConditionalOnProperty(prefix = "mohs", name = "enabled", matchIfMissing = true)
@ConditionalOnClass(HealthIndicator.class)
public class MohsHealthAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(name = "mohsHealthIndicator")
    @ConditionalOnEnabledHealthIndicator("mohs")
    public MohsHealthIndicator mohsHealthIndicator(Mohs mohs) {
        return new MohsHealthIndicator(mohs.lifecycle());
    }
}
