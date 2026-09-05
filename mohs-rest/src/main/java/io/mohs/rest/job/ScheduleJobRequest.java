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
package io.mohs.rest.job;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import org.jspecify.annotations.Nullable;

import io.mohs.core.execution.Priority;

/**
 * The body of {@code POST /jobs/{jobKey}/schedule} — {@code ScheduleCommand}'s three terminals at
 * the REST boundary: {@code at} (an absolute time), {@code delay} (an ISO-8601 duration from now,
 * computed on the server and therefore immune to client clock skew), and both absent meaning now.
 *
 * <p>They are mutually exclusive: "when it runs" OR "how long from now", never both.
 * {@code priority} absent means {@code NORMAL} (the engine's default).
 *
 * <p>The defensive copy of {@code payload} uses {@code new LinkedHashMap<>(...)} wrapped in
 * {@code Collections.unmodifiableMap} rather than {@code Map.copyOf} — a deliberate divergence from
 * the project's usual {@code List.copyOf}/{@code Map.copyOf} pattern, because {@code Map.copyOf}
 * rejects {@code null} values and JSON legitimately has them (e.g. {@code {"a": null}}).
 *
 * @param payload the input passed to the job handler
 * @param at the requested firing instant, or {@code null} when using a delay or immediate scheduling
 * @param delay the delay before the scheduled instant
 * @param priority the ordering priority used when claiming work
 */
public record ScheduleJobRequest(Map<String, Object> payload, @Nullable Instant at, @Nullable Duration delay,
        @Nullable Priority priority) {

    /**
     * Creates a {@code ScheduleJobRequest} with the supplied values.
     *
     * @param payload the input passed to the job handler
     * @param at the requested firing instant, or {@code null} when using a delay or immediate scheduling
     * @param delay the delay before the scheduled instant
     * @param priority the ordering priority used when claiming work
     */
    public ScheduleJobRequest {
        Objects.requireNonNull(payload, "payload");
        payload = Collections.unmodifiableMap(new LinkedHashMap<>(payload));
        // Validated in the record's constructor: it fires during deserialisation and the
        // RestExceptionHandler returns a 422 carrying this message (the house pattern)
        if (at != null && delay != null) {
            throw new IllegalArgumentException("at and delay are mutually exclusive — pick WHEN it runs or HOW LONG from now, not both");
        }
        // A negative delay would silently become at(now - X) — an execution immediately due, with no warning
        if (delay != null && delay.isNegative()) {
            throw new IllegalArgumentException("delay must not be negative — use 'at' for an absolute time, or omit both for now");
        }
    }
}
