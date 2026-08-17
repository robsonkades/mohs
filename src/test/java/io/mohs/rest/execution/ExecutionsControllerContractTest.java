package io.mohs.rest.execution;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import io.mohs.core.Mohs;
import io.mohs.core.execution.Execution;
import io.mohs.core.execution.ExecutionId;
import io.mohs.core.execution.ExecutionState;
import io.mohs.core.job.JobKey;
import io.mohs.rest.ApiPaths;
import io.mohs.rest.error.RestExceptionHandler;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** Ver Javadoc de {@link io.mohs.rest.job.JobsControllerContractTest} — mesmo padrão de teste. */
@WebMvcTest(properties = "mohs.enabled=false")
class ExecutionsControllerContractTest {

    private static final Instant NOW = Instant.parse("2026-08-15T12:00:00Z");

    @TestConfiguration
    static class ControllerConfig {

        @Bean
        ExecutionsController executionsController(Mohs mohs) {
            return new ExecutionsController(mohs);
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

    @Test
    void searchSignalsANextPageViaCursorWhenThereAreMoreRows() throws Exception {
        JobKey key = JobKey.of("welcome-email");
        Execution first = new Execution(ExecutionId.of("exec-2"), key, ExecutionState.ENQUEUED, NOW, null, List.of(), "tester");
        Execution second = new Execution(ExecutionId.of("exec-1"), key, ExecutionState.ENQUEUED, NOW.minusSeconds(1), null, List.of(), "tester");
        when(mohs.executions(any())).thenReturn(List.of(first, second));

        mockMvc.perform(get(ApiPaths.V1 + "/executions")
                        .param("status", "ENQUEUED").param("jobKey", "welcome-email").param("size", "1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.items.length()").value(1))
                .andExpect(jsonPath("$.items[0].executionId").value("exec-2"))
                .andExpect(jsonPath("$.nextCursor").value("exec-2"));
    }

    /** size=0/-1 saturam em 1 (CursorPage.clampSize) — antes, 0 estourava IndexOutOfBounds e negativo estourava IAE, ambos 500. */
    @Test
    void searchSaturatesANonPositiveSizeInsteadOfFailing() throws Exception {
        JobKey key = JobKey.of("welcome-email");
        Execution first = new Execution(ExecutionId.of("exec-2"), key, ExecutionState.ENQUEUED, NOW, null, List.of(), "tester");
        Execution second = new Execution(ExecutionId.of("exec-1"), key, ExecutionState.ENQUEUED, NOW.minusSeconds(1), null, List.of(), "tester");
        when(mohs.executions(any())).thenReturn(List.of(first, second));

        mockMvc.perform(get(ApiPaths.V1 + "/executions").param("size", "0"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.items.length()").value(1));

        mockMvc.perform(get(ApiPaths.V1 + "/executions").param("size", "-1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.items.length()").value(1));
    }

    @Test
    void getReturnsTheExecution() throws Exception {
        JobKey key = JobKey.of("welcome-email");
        Execution execution = new Execution(ExecutionId.of("exec-1"), key, ExecutionState.SUCCEEDED, NOW, NOW, List.of(), "tester");
        when(mohs.findExecution(ExecutionId.of("exec-1"))).thenReturn(Optional.of(execution));

        mockMvc.perform(get(ApiPaths.V1 + "/executions/exec-1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.executionId").value("exec-1"))
                .andExpect(jsonPath("$.state").value("SUCCEEDED"));
    }

    @Test
    void getUnknownExecutionReturns404() throws Exception {
        when(mohs.findExecution(ExecutionId.of("ghost"))).thenReturn(Optional.empty());

        mockMvc.perform(get(ApiPaths.V1 + "/executions/ghost"))
                .andExpect(status().isNotFound());
    }

    /** ADR-0034: 202 com o estado corrente (não necessariamente terminal) e Location apontando pro detalhe — cancel é aceito, nunca garantido. */
    @Test
    void cancelReturns202WithTheCurrentStateAndALocationHeader() throws Exception {
        JobKey key = JobKey.of("welcome-email");
        Execution running = new Execution(ExecutionId.of("exec-1"), key, ExecutionState.RUNNING, NOW, NOW, List.of(), "tester");
        when(mohs.cancel(ExecutionId.of("exec-1"))).thenReturn(Optional.of(running));

        mockMvc.perform(post(ApiPaths.V1 + "/executions/exec-1/cancel"))
                .andExpect(status().isAccepted())
                .andExpect(header().string("Location", ApiPaths.V1 + "/executions/exec-1"))
                .andExpect(jsonPath("$.executionId").value("exec-1"))
                .andExpect(jsonPath("$.state").value("RUNNING"));
    }

    @Test
    void cancelOfAnUnknownExecutionReturns404() throws Exception {
        when(mohs.cancel(ExecutionId.of("ghost"))).thenReturn(Optional.empty());

        mockMvc.perform(post(ApiPaths.V1 + "/executions/ghost/cancel"))
                .andExpect(status().isNotFound());
    }

    /** ADR-0033/M3: retry manual — 202 com o recibo da MESMA execução rearmada (actor original) e Location pro detalhe. */
    @Test
    void retryReturns202WithTheRearmedExecutionAndALocationHeader() throws Exception {
        JobKey key = JobKey.of("welcome-email");
        Execution rearmed = new Execution(ExecutionId.of("exec-1"), key, ExecutionState.RETRY_SCHEDULED, NOW, null, List.of(), "tester");
        when(mohs.retry(ExecutionId.of("exec-1"))).thenReturn(Optional.of(rearmed));

        mockMvc.perform(post(ApiPaths.V1 + "/executions/exec-1/retry"))
                .andExpect(status().isAccepted())
                .andExpect(header().string("Location", ApiPaths.V1 + "/executions/exec-1"))
                .andExpect(jsonPath("$.executionId").value("exec-1"))
                .andExpect(jsonPath("$.jobKey").value("welcome-email"))
                .andExpect(jsonPath("$.actor").value("tester"));
    }

    @Test
    void retryOfAnUnknownExecutionReturns404() throws Exception {
        when(mohs.retry(ExecutionId.of("ghost"))).thenReturn(Optional.empty());

        mockMvc.perform(post(ApiPaths.V1 + "/executions/ghost/retry"))
                .andExpect(status().isNotFound());
    }

    /** Estado ≠ FAILED vira 409 que ensina — a ISE do guard de estado da fachada nunca escapa como 500. */
    @Test
    void retryOfANonFailedExecutionReturns409NamingTheState() throws Exception {
        when(mohs.retry(ExecutionId.of("exec-1")))
                .thenThrow(new IllegalStateException("execution exec-1 is RUNNING — only FAILED executions can be manually retried"));

        mockMvc.perform(post(ApiPaths.V1 + "/executions/exec-1/retry"))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.title").value("Execution not retryable"))
                .andExpect(jsonPath("$.detail").value(containsString("RUNNING")));
    }
}
