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
