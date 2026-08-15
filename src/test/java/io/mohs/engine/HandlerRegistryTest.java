package io.mohs.engine;

import org.junit.jupiter.api.Test;

import io.mohs.core.job.JobKey;

import static org.assertj.core.api.Assertions.assertThat;

class HandlerRegistryTest {

    @Test
    void registerWithoutPayloadTypeLeavesItEmpty() {
        HandlerRegistry registry = new HandlerRegistry();
        registry.register(JobKey.of("welcome-email"), (payload, ctx) -> { });

        assertThat(registry.payloadType(JobKey.of("welcome-email"))).isEmpty();
    }

    @Test
    void registerWithPayloadTypeIsFindable() {
        HandlerRegistry registry = new HandlerRegistry();
        registry.register(JobKey.of("welcome-email"), (payload, ctx) -> { }, String.class);

        assertThat(registry.payloadType(JobKey.of("welcome-email"))).contains(String.class);
    }

    @Test
    void unknownJobHasNoPayloadType() {
        HandlerRegistry registry = new HandlerRegistry();

        assertThat(registry.payloadType(JobKey.of("ghost"))).isEmpty();
    }

    @Test
    void findStillResolvesTheHandlerRegardlessOfPayloadType() throws Exception {
        HandlerRegistry registry = new HandlerRegistry();
        registry.register(JobKey.of("welcome-email"), (payload, ctx) -> { }, String.class);

        assertThat(registry.find(JobKey.of("welcome-email"))).isPresent();
    }
}
