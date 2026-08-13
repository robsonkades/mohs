package io.mohs.rest.job;

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

        ScheduleJobRequest request = new ScheduleJobRequest(payload, null);

        assertThat(request.payload()).containsEntry("name", null);
    }

    @Test
    void copiesPayloadDefensively() {
        Map<String, Object> mutable = new HashMap<>(Map.of("a", 1));
        ScheduleJobRequest request = new ScheduleJobRequest(mutable, Instant.now());

        mutable.put("b", 2);

        assertThat(request.payload()).containsOnlyKeys("a");
    }

    @Test
    void rejectsNullPayload() {
        assertThatThrownBy(() -> new ScheduleJobRequest(null, null))
                .isInstanceOf(NullPointerException.class);
    }
}
