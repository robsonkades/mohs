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

/**
 * A {@link JobDefinition} plus the operational state kept distinct from the definitional one —
 * {@code orphaned}/{@code paused} are deliberately not fields of {@link JobDefinition} (an upsert
 * never touches them); this type exists only for a combined read in {@link JobStore}.
 *
 * <p>{@code nextFireAt} is the trigger's state: {@code null} means nothing to fire — on-demand, or
 * fixed-delay awaiting the end of the previous execution. The per-job mutex counter of an earlier
 * era is gone: the cap now derives from {@code mohs_lease} ({@link LeaseStore#countByJob}) — a live
 * ownership IS the occupied slot.
 */
public record StoredJob(JobDefinition definition, boolean orphaned, boolean paused,
        @Nullable Instant nextFireAt) {

    public StoredJob {
        Objects.requireNonNull(definition, "definition");
    }
}
