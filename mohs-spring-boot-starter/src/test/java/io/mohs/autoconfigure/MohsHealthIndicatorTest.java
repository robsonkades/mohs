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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.springframework.boot.health.contributor.Status;

import io.mohs.core.EngineState;
import io.mohs.core.MohsLifecycle;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * The mapping is the whole contract, so the table is the test — and it covers every constant of
 * {@link EngineState}, because a new state added without a decision here would silently inherit
 * whatever the {@code switch} does with it (nothing: the compiler refuses an inexhaustive switch,
 * which is the point of enumerating them one by one).
 */
class MohsHealthIndicatorTest {

    @ParameterizedTest(name = "{0} -> {1}")
    @CsvSource({
            "RUNNING,  UP",
            "PAUSED,   OUT_OF_SERVICE",
            "DRAINING, OUT_OF_SERVICE",
            "CREATED,  DOWN",
            "STOPPED,  DOWN",
    })
    void mapsTheEngineStateToAStatusAndReportsItAsADetail(EngineState state, String expectedStatus) {
        MohsLifecycle lifecycle = mock(MohsLifecycle.class);
        when(lifecycle.state()).thenReturn(state);

        var health = new MohsHealthIndicator(lifecycle).health();

        assertThat(health.getStatus()).isEqualTo(new Status(expectedStatus));
        assertThat(health.getDetails()).containsEntry("state", state.name());
    }
}
