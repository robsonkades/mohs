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
import java.util.Objects;

import org.jspecify.annotations.Nullable;

import io.mohs.core.definition.JobDefinition;

/** A retained active definition and its authoritative trigger state. Null nextFireAt means disarmed. */
public record StoredJob(JobDefinition definition, boolean paused,
        @Nullable Instant nextFireAt, long scheduleRevision) {

    /**
     * Creates a {@code StoredJob} with the supplied values.
     *
     * @param definition the registered job definition

     * @param paused whether automatic firing is suspended
     * @param nextFireAt the next automatic firing instant, or {@code null} when disarmed
     * @param scheduleRevision the current series generation
     */
    public StoredJob {
        Objects.requireNonNull(definition, "definition");
        if (nextFireAt != null) nextFireAt = io.mohs.core.TimePrecision.eligible(nextFireAt);
        if (scheduleRevision < 1) throw new IllegalArgumentException("scheduleRevision must be positive");
    }
}
