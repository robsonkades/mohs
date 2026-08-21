package io.mohs.demo;

import io.mohs.core.definition.OnDemandJob;
import io.mohs.core.definition.RecurringJob;
import io.mohs.core.resource.RateLimit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
public class Demo {

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
}
