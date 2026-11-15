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
    void frozenCatalogRejectsMutationAndIgnoresLocalClassNamesInItsFingerprint() {
        var before = new HandlerRegistry();
        before.register(JobKey.of("job"), (payload, ctx) -> {}, String.class, "format-v1");
        var afterRename = new HandlerRegistry();
        afterRename.register(JobKey.of("job"), (payload, ctx) -> {}, Object.class, "format-v1");
        assertThat(before.freeze().fingerprint()).isEqualTo(afterRename.freeze().fingerprint());
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> before.register(JobKey.of("new-job"),
                (payload, ctx) -> {}, Object.class)).isInstanceOf(IllegalStateException.class).hasMessageContaining("frozen");
    }

    @Test
    void noInputHasAnExplicitLocalTypeAndDurableContract() {
        HandlerRegistry registry = new HandlerRegistry();
        registry.register(JobKey.of("welcome-email"), (payload, ctx) -> { }, io.mohs.core.NoInput.class);

        assertThat(registry.payloadType(JobKey.of("welcome-email"))).contains(io.mohs.core.NoInput.class);
        assertThat(registry.reference(JobKey.of("welcome-email")).inputContract()).isEqualTo("welcome-email");
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
