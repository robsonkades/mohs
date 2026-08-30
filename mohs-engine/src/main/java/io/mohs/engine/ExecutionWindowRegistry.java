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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mohs.core.resource.ExecutionWindow;

/**
 * Lookup of an {@link ExecutionWindow} by name — far simpler than {@link RunnerRegistry}: an
 * {@code ExecutionWindow} owns no resource to build or close (there is no {@code Executor} behind
 * it, unlike {@link io.mohs.core.resource.MohsRunner}), so this class has no lifecycle of its own.
 *
 * <p>There is no property-based path — {@link ExecutionWindow}'s own Javadoc already documents that
 * predicates exist only in code, so only a {@code @Bean ExecutionWindow} feeds this list.
 */
public final class ExecutionWindowRegistry {

    private static final Logger log = LoggerFactory.getLogger(ExecutionWindowRegistry.class);

    private final Map<String, ExecutionWindow> windows;

    public ExecutionWindowRegistry(List<ExecutionWindow> windows) {
        Objects.requireNonNull(windows, "windows");
        Map<String, ExecutionWindow> byName = new LinkedHashMap<>();
        for (ExecutionWindow window : windows) {
            if (byName.containsKey(window.name())) {
                throw new IllegalArgumentException("duplicate execution window name '" + window.name() + "'");
            }
            byName.put(window.name(), window);
        }
        this.windows = Map.copyOf(byName);
    }

    /**
     * {@code null} never excludes — a job referencing no window at all. An unknown name also
     * excludes: fail-safe, since a typo in the configuration must not let a job slip past the
     * intended exclusion window (silently permitting would be worse than blocking until the
     * configuration is fixed) — with a WARN on every occurrence.
     */
    public boolean excludes(@Nullable String windowName, Instant now) {
        if (windowName == null) {
            return false;
        }
        ExecutionWindow window = windows.get(windowName);
        if (window == null) {
            log.warn("job references unknown execution window '{}' — treating as excluded (fail-safe) until fixed", windowName);
            return true;
        }
        return window.excludes(now);
    }
}
