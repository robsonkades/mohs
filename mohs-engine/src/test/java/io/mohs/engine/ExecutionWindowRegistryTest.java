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

import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.mohs.core.resource.ExecutionWindow;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ExecutionWindowRegistryTest {

    private static final Instant NOW = Instant.parse("2026-08-15T12:00:00Z");

    @Test
    void nullWindowNameNeverExcludes() {
        ExecutionWindowRegistry registry = new ExecutionWindowRegistry(List.of());

        assertThat(registry.excludes(null, NOW)).isFalse();
    }

    @Test
    void unknownWindowNameExcludesFailSafe() {
        ExecutionWindowRegistry registry = new ExecutionWindowRegistry(List.of());

        assertThat(registry.excludes("ghost-window", NOW)).isTrue();
    }

    @Test
    void knownWindowExcludesWhenPredicateMatches() {
        ExecutionWindow alwaysExcludes = ExecutionWindow.named("maintenance").exclude(instant -> true).build();
        ExecutionWindowRegistry registry = new ExecutionWindowRegistry(List.of(alwaysExcludes));

        assertThat(registry.excludes("maintenance", NOW)).isTrue();
    }

    @Test
    void knownWindowDoesNotExcludeWhenPredicateDoesNotMatch() {
        ExecutionWindow neverExcludes = ExecutionWindow.named("maintenance").exclude(instant -> false).build();
        ExecutionWindowRegistry registry = new ExecutionWindowRegistry(List.of(neverExcludes));

        assertThat(registry.excludes("maintenance", NOW)).isFalse();
    }

    @Test
    void constructorRejectsDuplicateWindowNames() {
        ExecutionWindow a = ExecutionWindow.named("maintenance").exclude(instant -> true).build();
        ExecutionWindow b = ExecutionWindow.named("maintenance").exclude(instant -> false).build();

        assertThatThrownBy(() -> new ExecutionWindowRegistry(List.of(a, b)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("duplicate")
                .hasMessageContaining("maintenance");
    }
}
