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

/** Ver Javadoc de {@link io.mohs.rest.job.JobsControllerContractTest} — mesmo padrão de teste. */
@WebMvcTest(properties = "mohs.enabled=false")
class OverviewControllerContractTest {

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

    /** A resposta é a forma plana do OverviewSnapshot: só os estados vivos no mapa (sempre os três, zero incluso) e a janela viajando junto da vazão. */
    @Test
    void overviewReturnsLiveCountsAndTheWindowedThroughput() throws Exception {
        when(mohs.overview(OverviewController.DEFAULT_THROUGHPUT_WINDOW)).thenReturn(new OverviewSnapshot(
                Map.of(ExecutionState.ENQUEUED, 12L, ExecutionState.RUNNING, 3L),
                Duration.ofSeconds(60), 240L, 5L));

        mockMvc.perform(get(ApiPaths.V1 + "/overview"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.executionCountsByStatus.length()").value(3))
                .andExpect(jsonPath("$.executionCountsByStatus.ENQUEUED").value(12))
                .andExpect(jsonPath("$.executionCountsByStatus.RUNNING").value(3))
                .andExpect(jsonPath("$.executionCountsByStatus.RETRY_SCHEDULED").value(0))
                .andExpect(jsonPath("$.throughput.window").value("PT1M"))
                .andExpect(jsonPath("$.throughput.succeeded").value(240))
                .andExpect(jsonPath("$.throughput.failed").value(5));
    }

    /** {@code ?window=} no estilo simples do Boot: 15m vira PT15M na fachada e viaja de volta na resposta. */
    @Test
    void windowParameterSelectsTheThroughputWindow() throws Exception {
        when(mohs.overview(Duration.ofMinutes(15))).thenReturn(new OverviewSnapshot(
                Map.of(), Duration.ofMinutes(15), 900L, 12L));

        mockMvc.perform(get(ApiPaths.V1 + "/overview").queryParam("window", "15m"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.throughput.window").value("PT15M"))
                .andExpect(jsonPath("$.throughput.succeeded").value(900));
    }

    /** O clamp protege o "barato por construção": acima do teto, a janela aplicada é o teto — e o cliente a VÊ na resposta. */
    @Test
    void windowParameterIsClampedToTheMax() throws Exception {
        when(mohs.overview(OverviewController.MAX_THROUGHPUT_WINDOW)).thenReturn(new OverviewSnapshot(
                Map.of(), OverviewController.MAX_THROUGHPUT_WINDOW, 0L, 0L));

        mockMvc.perform(get(ApiPaths.V1 + "/overview").queryParam("window", "48h"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.throughput.window").value("PT1H"));
    }

    /** Janela imparseável é 422 que ensina (problem+json com o campo), nunca 500 — parse explícito, sem depender do binder do host. */
    @Test
    void anUnparseableWindowIsA422ThatTeaches() throws Exception {
        mockMvc.perform(get(ApiPaths.V1 + "/overview").queryParam("window", "banana"))
                .andExpect(status().isUnprocessableContent())
                .andExpect(jsonPath("$.field").value("window"))
                .andExpect(jsonPath("$.detail").value(containsString("15m")));
    }

    /** SSE snapshot stream: o subscribe já entrega o retrato inicial — um evento nomeado por tipo, cliente assina só o que consome. */
    @Test
    void streamEmitsTheInitialSnapshotAsNamedEvents() throws Exception {
        when(mohs.overview(OverviewController.DEFAULT_THROUGHPUT_WINDOW)).thenReturn(new OverviewSnapshot(
                Map.of(ExecutionState.ENQUEUED, 4L), Duration.ofSeconds(60), 7L, 1L));
        when(mohs.jobs()).thenReturn(List.of());
        when(mohs.nodes()).thenReturn(List.of(new NodeSnapshot("node-1", EngineState.RUNNING, NOW)));
        when(mohs.executions(any())).thenReturn(List.of());

        MvcResult result = mockMvc.perform(get(ApiPaths.V1 + "/overview/stream"))
                .andExpect(request().asyncStarted())
                .andExpect(status().isOk())
                .andReturn();

        String body = result.getResponse().getContentAsString();
        assertThat(body).contains("event:overview", "event:jobs", "event:nodes", "event:executions");
        // envelope {asOf, data}: o carimbo do retrato (Clock injetado) viaja em todo evento
        assertThat(body).contains("\"asOf\":\"2026-08-17T12:00:00Z\"");
        assertThat(body).contains("\"succeeded\":7");
        assertThat(body).contains("node-1");
    }
}
