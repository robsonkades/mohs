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

    /** "Quando roda" OU "daqui a quanto" — os dois juntos é indecisão do cliente, rejeitada com mensagem que ensina. */
    @Test
    void rejectsAtAndDelayTogether() {
        assertThatThrownBy(() -> new ScheduleJobRequest(Map.of(), Instant.now(), Duration.ofMinutes(5), null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("mutually exclusive");
    }

    /** delay negativo viraria at(now - X) em silêncio — execução imediatamente devida sem aviso. */
    @Test
    void rejectsANegativeDelay() {
        assertThatThrownBy(() -> new ScheduleJobRequest(Map.of(), null, Duration.ofMinutes(-5), null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must not be negative");
    }
}
