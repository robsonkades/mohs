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
package io.mohs.demo;

import io.mohs.core.definition.OnDemandJob;
import io.mohs.core.definition.RecurringJob;
import io.mohs.core.resource.RateLimit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * Enables the demonstration jobs for the demo profile.
 */
@Component
public class Demo {

    /**
     * Creates a {@code Demo} instance.
     */
    public Demo() {
    }

    @Bean
    RateLimit smtpRateLimit() {
        return new RateLimit("demo", 100, Duration.ofMinutes(1));
    }

    private static final Logger log = LoggerFactory.getLogger(Demo.class);

    @RecurringJob(id = "every-job", every = "PT1S", retries = 10)
    void everyMethod() {
     log.info("Hello, world!");
    }

    @OnDemandJob(id = "every-job2", retries = 10)
    void everyMethod2() {
        log.info("Hello, world!");
    }

    /**
     * Load-bench handler: lease renewal is load-dependent and only misbehaves under sustained
     * in-flight work, so this handler is slow on purpose and keeps roughly dispatch-concurrency
     * executions in flight during a drain. The sleep is a deliberate bench wait, not
     * synchronisation.
     */
    @OnDemandJob(id = "slow-job", retries = 10)
    void slowMethod() throws InterruptedException {
        Thread.sleep(500);
    }
}
