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
package io.mohs.core;

import java.time.Instant;
import java.util.Objects;

import org.jspecify.annotations.Nullable;

import io.mohs.core.definition.JobDefinition;

/**
 * A {@link JobDefinition} plus the operational state it deliberately does not carry
 * ({@code paused}) and the computed next firing ({@code nextFireAt}) — the read form returned by
 * {@link Mohs#findJob}/{@link Mohs#jobs}.
 *
 * <p>It is the public mirror of {@code io.mohs.engine.StoredJob}, without {@code orphaned} and
 * {@code runningExecutionCount} (too operational to expose through the API yet, with no consumer
 * outside the engine needing them).
 *
 * @param nextFireAt the next firing instant, or {@code null} when paused, on demand or disarmed
 * @param definition the registered job definition
 * @param paused whether automatic firing is suspended
 */
public record JobSnapshot(JobDefinition definition, boolean paused, @Nullable Instant nextFireAt) {

    /**
     * Creates a {@code JobSnapshot} with the supplied values.
     *
     * @param definition the registered job definition
     * @param paused whether automatic firing is suspended
     * @param nextFireAt the next firing instant, or {@code null} when paused, on demand or disarmed
     */
    public JobSnapshot {
        Objects.requireNonNull(definition, "definition");
    }
}
