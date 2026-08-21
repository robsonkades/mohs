package io.mohs.rest.runner;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.context.annotation.Bean;
import org.springframework.test.web.servlet.MockMvc;

import io.mohs.core.Mohs;
import io.mohs.core.RunnerSnapshot;
import io.mohs.core.resource.RunnerMode;
import io.mohs.rest.ApiPaths;
import io.mohs.rest.error.RestExceptionHandler;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Ver Javadoc de {@link io.mohs.rest.job.JobsControllerContractTest} sobre
 * o {@code @TestConfiguration}/{@code mohs.enabled=false} — mesmo motivo
 * aqui: {@link io.mohs.rest.RestSliceConfiguration} não escaneia
 * {@code io.mohs.rest} no component-scan, então controller e
 * {@link RestExceptionHandler} entram via {@code @Bean} explícito.
 */
@WebMvcTest(properties = "mohs.enabled=false")
class RunnersControllerContractTest {

    private static final Mohs MOHS = mock(Mohs.class);

    @TestConfiguration
    static class ControllerConfig {

        @Bean
        RunnersController runnersController() {
            return new RunnersController(MOHS);
        }

        @Bean
        RestExceptionHandler restExceptionHandler() {
            return new RestExceptionHandler();
        }
    }

    @Autowired
    private MockMvc mockMvc;

    /** O contrato do design doc: nome, modo, teto e ocupação — nada de cursor, a lista é o que o boot declarou. */
    @Test
    void listExposesModeMaxAndRunningPerRunner() throws Exception {
        when(MOHS.runners()).thenReturn(List.of(
                new RunnerSnapshot("cpu-heavy", RunnerMode.CPU, 8, 3),
                new RunnerSnapshot("io", RunnerMode.IO, 200, 17)));

        mockMvc.perform(get(ApiPaths.V1 + "/runners"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(2))
                .andExpect(jsonPath("$[0].name").value("cpu-heavy"))
                .andExpect(jsonPath("$[0].mode").value("CPU"))
                .andExpect(jsonPath("$[0].max").value(8))
                .andExpect(jsonPath("$[0].running").value(3))
                .andExpect(jsonPath("$[1].name").value("io"))
                .andExpect(jsonPath("$[1].mode").value("IO"));
    }

    /** Node sem runner declarado é impossível (o registry exige o default), mas lista vazia não pode virar 500. */
    @Test
    void listOfNoRunnersIsAnEmptyArray() throws Exception {
        when(MOHS.runners()).thenReturn(List.of());

        mockMvc.perform(get(ApiPaths.V1 + "/runners"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(0));
    }
}
