package io.mohs.rest.job;

import java.time.Duration;
import java.time.ZoneId;

import org.junit.jupiter.api.Test;

import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Round-trip do discriminador {@code type} através do {@link ScheduleView}
 * selado. Usa {@link JsonMapper} direto (Jackson 3, já transitivo via
 * {@code spring-boot-starter-webmvc}) em vez de {@code @JsonTest} — o
 * {@code spring-boot-starter-jackson-test} que fornece {@code @JsonTest}
 * não é dependência deste projeto, e não é o caso de adicionar uma só pra
 * isto.
 */
class ScheduleViewJsonTest {

    private final JsonMapper mapper = JsonMapper.builder().build();

    @Test
    void roundTripsCronView() {
        ScheduleView original = new CronView("0 0 2 * * *", ZoneId.of("America/Sao_Paulo"));

        String json = mapper.writeValueAsString(original);
        ScheduleView parsed = mapper.readValue(json, ScheduleView.class);

        assertThat(json).contains("\"type\"", "\"CRON\"");
        assertThat(parsed).isEqualTo(original).isInstanceOf(CronView.class);
    }

    @Test
    void roundTripsIntervalView() {
        ScheduleView original = new IntervalView(Duration.ofMinutes(30), true);

        String json = mapper.writeValueAsString(original);
        ScheduleView parsed = mapper.readValue(json, ScheduleView.class);

        assertThat(json).contains("\"type\"", "\"INTERVAL\"");
        assertThat(parsed).isEqualTo(original).isInstanceOf(IntervalView.class);
    }

    @Test
    void roundTripsOnDemandView() {
        ScheduleView original = new OnDemandView();

        String json = mapper.writeValueAsString(original);
        ScheduleView parsed = mapper.readValue(json, ScheduleView.class);

        assertThat(json).contains("\"type\"", "\"ON_DEMAND\"");
        assertThat(parsed).isEqualTo(original).isInstanceOf(OnDemandView.class);
    }
}
