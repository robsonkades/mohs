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
import java.time.ZoneId;

import org.junit.jupiter.api.Test;

import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A round trip of the {@code type} discriminator through the sealed {@link ScheduleView}.
 *
 * <p>It uses {@link JsonMapper} directly (Jackson 3, already transitive through
 * {@code spring-boot-starter-webmvc}) rather than {@code @JsonTest}: the
 * {@code spring-boot-starter-jackson-test} that provides {@code @JsonTest} is not a dependency of
 * this project, and this is not reason enough to add one.
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
