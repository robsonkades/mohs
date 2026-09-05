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
package io.mohs.rest.overview;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import io.mohs.core.EngineState;
import io.mohs.core.Mohs;
import io.mohs.core.NodeSnapshot;
import io.mohs.core.OverviewSnapshot;
import io.mohs.core.ThroughputReading;
import io.mohs.core.execution.ExecutionState;
import io.mohs.rest.ApiPaths;
import io.mohs.rest.error.RestExceptionHandler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.request;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** See {@link io.mohs.rest.job.JobsControllerContractTest}'s Javadoc — the same test pattern. */
@WebMvcTest(properties = "mohs.enabled=false")
class OverviewControllerContractTest {

    /** The overview's short reading — fixed; these tests verify the long one. */
    private static final ThroughputReading RECENT = new ThroughputReading(Duration.ofSeconds(10), 0L, 0L);

    private static final Instant NOW = Instant.parse("2026-08-17T12:00:00Z");

    @TestConfiguration
    static class ControllerConfig {

        @Bean
        OverviewStreamBroadcaster overviewStreamBroadcaster(Mohs mohs) {
            return new OverviewStreamBroadcaster(mohs, Clock.fixed(NOW, ZoneOffset.UTC));
        }

        @Bean
        OverviewController overviewController(Mohs mohs, OverviewStreamBroadcaster broadcaster) {
            return new OverviewController(mohs, broadcaster);
        }

        @Bean
        RestExceptionHandler restExceptionHandler() {
            return new RestExceptionHandler();
        }
    }

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private Mohs mohs;

    /** The response is the flat form of OverviewSnapshot: only live states in the map (always all three, zeros included), with the window travelling alongside the throughput. */
    @Test
    void overviewReturnsLiveCountsAndTheWindowedThroughput() throws Exception {
        when(mohs.overview(OverviewController.DEFAULT_THROUGHPUT_WINDOW)).thenReturn(new OverviewSnapshot(
                Map.of(ExecutionState.ENQUEUED, 12L, ExecutionState.RUNNING, 3L),
                new ThroughputReading(Duration.ofSeconds(60), 240L, 5L),
                new ThroughputReading(Duration.ofSeconds(10), 47L, 3L)));

        mockMvc.perform(get(ApiPaths.V1 + "/overview"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.executionCountsByStatus.length()").value(3))
                .andExpect(jsonPath("$.executionCountsByStatus.ENQUEUED").value(12))
                .andExpect(jsonPath("$.executionCountsByStatus.RUNNING").value(3))
                .andExpect(jsonPath("$.executionCountsByStatus.RETRY_WAITING").value(0))
                .andExpect(jsonPath("$.throughput.window").value("PT1M"))
                .andExpect(jsonPath("$.throughput.succeeded").value(240))
                .andExpect(jsonPath("$.throughput.failed").value(5))
                // The SHORT reading and the pre-computed ratePerSecond are why this contract
                // changed — without them the dashboard has only instantaneous gauges, which sit at
                // zero for a fast job (Little's Law). The rate travels computed because a client
                // that gets the division wrong draws a wrong chart with nothing to flag it
                .andExpect(jsonPath("$.throughput.ratePerSecond").value(245.0 / 60))
                .andExpect(jsonPath("$.recent.window").value("PT10S"))
                .andExpect(jsonPath("$.recent.succeeded").value(47))
                .andExpect(jsonPath("$.recent.failed").value(3))
                .andExpect(jsonPath("$.recent.ratePerSecond").value(5.0));
    }

    /** {@code ?window=} in Boot's simple style: 15m becomes PT15M at the facade and travels back in the response. */
    @Test
    void windowParameterSelectsTheThroughputWindow() throws Exception {
        when(mohs.overview(Duration.ofMinutes(15))).thenReturn(new OverviewSnapshot(
                Map.of(), new ThroughputReading(Duration.ofMinutes(15), 900L, 12L), RECENT));

        mockMvc.perform(get(ApiPaths.V1 + "/overview").queryParam("window", "15m"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.throughput.window").value("PT15M"))
                .andExpect(jsonPath("$.throughput.succeeded").value(900));
    }

    /** The clamp protects "cheap by construction": above the ceiling, the applied window is the ceiling — and the client SEES it in the response. */
    @Test
    void windowParameterIsClampedToTheMax() throws Exception {
        when(mohs.overview(OverviewController.MAX_THROUGHPUT_WINDOW)).thenReturn(new OverviewSnapshot(
                Map.of(), new ThroughputReading(OverviewController.MAX_THROUGHPUT_WINDOW, 0L, 0L), RECENT));

        mockMvc.perform(get(ApiPaths.V1 + "/overview").queryParam("window", "48h"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.throughput.window").value("PT1H"));
    }

    /** An unparseable window is a 422 that teaches (problem+json naming the field), never a 500 — an explicit parse, not the host's binder. */
    @Test
    void anUnparseableWindowIsA422ThatTeaches() throws Exception {
        mockMvc.perform(get(ApiPaths.V1 + "/overview").queryParam("window", "banana"))
                .andExpect(status().isUnprocessableContent())
                .andExpect(jsonPath("$.field").value("window"))
                .andExpect(jsonPath("$.detail").value(containsString("15m")));
    }

    /** The SSE snapshot stream: subscribing already delivers the initial snapshot — one named event per type, so a client subscribes only to what it consumes. */
    @Test
    void streamEmitsTheInitialSnapshotAsNamedEvents() throws Exception {
        when(mohs.overview(OverviewController.DEFAULT_THROUGHPUT_WINDOW)).thenReturn(new OverviewSnapshot(
                Map.of(ExecutionState.ENQUEUED, 4L), new ThroughputReading(Duration.ofSeconds(60), 7L, 1L), RECENT));
        when(mohs.jobs()).thenReturn(List.of());
        when(mohs.nodes()).thenReturn(List.of(new NodeSnapshot("node-1", EngineState.RUNNING, NOW)));
        when(mohs.executions(any())).thenReturn(List.of());

        MvcResult result = mockMvc.perform(get(ApiPaths.V1 + "/overview/stream"))
                .andExpect(request().asyncStarted())
                .andExpect(status().isOk())
                .andReturn();

        String body = result.getResponse().getContentAsString();
        assertThat(body).contains("event:runners", "event:overview", "event:jobs", "event:nodes", "event:executions");
        // The {asOf, data} envelope: the snapshot's stamp (from the injected clock) travels on every event
        assertThat(body).contains("\"asOf\":\"2026-08-17T12:00:00Z\"");
        assertThat(body).contains("\"succeeded\":7");
        assertThat(body).contains("node-1");
    }
}
