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

import io.mohs.core.execution.Execution;
import io.mohs.core.execution.ExecutionState;
import io.mohs.core.job.JobKey;

/**
 * The filter and pagination of {@link Mohs#executions} — a parameter object (Fowler, "Introduce
 * Parameter Object") for the six filters and controls of {@code GET /executions} in the REST design,
 * all optional except {@code limit}.
 *
 * <p>{@code cursor} is opaque: the {@code id} (UUIDv7, chronologically sortable) of the previous
 * page's last item. Results are ordered by descending {@code id} (most recent first), and only
 * {@link Execution}s with {@code id < cursor} are returned when it is present.
 *
 * @param limit the ceiling on items returned, at least 1
 */
public record ExecutionQuery(
        @Nullable JobKey jobKey,
        @Nullable ExecutionState status,
        @Nullable Instant from,
        @Nullable Instant to,
        @Nullable String cursor,
        int limit) {

    public ExecutionQuery {
        if (limit < 1) {
            throw new IllegalArgumentException("limit must be at least 1");
        }
    }
}
