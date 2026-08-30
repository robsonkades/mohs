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
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ScheduleJobRequestTest {

    @Test
    void nullValuedFieldSurvivesTheDefensiveCopy() {
        Map<String, Object> payload = new HashMap<>();
        payload.put("name", null);

        ScheduleJobRequest request = new ScheduleJobRequest(payload, null, null, null);

        assertThat(request.payload()).containsEntry("name", null);
    }

    @Test
    void copiesPayloadDefensively() {
        Map<String, Object> mutable = new HashMap<>(Map.of("a", 1));
        ScheduleJobRequest request = new ScheduleJobRequest(mutable, Instant.now(), null, null);

        mutable.put("b", 2);

        assertThat(request.payload()).containsOnlyKeys("a");
    }

    @Test
    void rejectsNullPayload() {
        assertThatThrownBy(() -> new ScheduleJobRequest(null, null, null, null))
                .isInstanceOf(NullPointerException.class);
    }

    /** "When it runs" OR "how long from now" — both together is client indecision, rejected with a message that teaches. */
    @Test
    void rejectsAtAndDelayTogether() {
        assertThatThrownBy(() -> new ScheduleJobRequest(Map.of(), Instant.now(), Duration.ofMinutes(5), null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("mutually exclusive");
    }

    /** A negative delay would silently become at(now - X) — an execution immediately due, with no warning. */
    @Test
    void rejectsANegativeDelay() {
        assertThatThrownBy(() -> new ScheduleJobRequest(Map.of(), null, Duration.ofMinutes(-5), null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must not be negative");
    }
}
