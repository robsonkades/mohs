package io.mohs.rest.batch;

import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.context.annotation.Bean;
import org.springframework.test.web.servlet.MockMvc;

import io.mohs.core.BatchSnapshot;
import io.mohs.core.Mohs;
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
 * aqui: {@link io.mohs.rest.RestSliceConfiguration} não escaneia {@code io.mohs.rest} no
 * component-scan, então controller e {@link RestExceptionHandler} entram
 * via {@code @Bean} explícito.
 */
@WebMvcTest(properties = "mohs.enabled=false")
class BatchesControllerContractTest {

    @TestConfiguration
    static class ControllerConfig {

        @Bean
        Mohs mohs() {
            Mohs mohs = mock(Mohs.class);
            when(mohs.findBatch("batch-1")).thenReturn(Optional.of(new BatchSnapshot("batch-1", 10, 7, 1)));
            when(mohs.findBatch("ghost")).thenReturn(Optional.empty());
            return mohs;
        }

        @Bean
        BatchesController batchesController(Mohs mohs) {
            return new BatchesController(mohs);
        }

        @Bean
        RestExceptionHandler restExceptionHandler() {
            return new RestExceptionHandler();
        }
    }

    @Autowired
    private MockMvc mockMvc;

    /**
     * {@code pending} e {@code state} não vêm do banco: são derivados dos três
     * contadores (ADR-0043), então não há como a resposta contradizer a si
     * mesma — 10 total com 8 terminados são 2 pendentes por construção, e o
     * lote só é {@code COMPLETED} quando esse resto é zero.
     */
    @Test
    void getReturnsTheCountersWithPendingAndStateDerived() throws Exception {
        mockMvc.perform(get(ApiPaths.V1 + "/batches/batch-1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.batchId").value("batch-1"))
                .andExpect(jsonPath("$.total").value(10))
                .andExpect(jsonPath("$.succeeded").value(7))
                .andExpect(jsonPath("$.failed").value(1))
                .andExpect(jsonPath("$.pending").value(2))
                .andExpect(jsonPath("$.state").value("RUNNING"));
    }

    @Test
    void getAnswers404ForAnUnknownBatch() throws Exception {
        mockMvc.perform(get(ApiPaths.V1 + "/batches/ghost"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.title").value("Batch not found"));
    }
}
