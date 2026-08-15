package io.mohs.rest.batch;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.context.annotation.Bean;
import org.springframework.test.web.servlet.MockMvc;

import io.mohs.rest.ApiPaths;
import io.mohs.rest.error.RestExceptionHandler;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Ver Javadoc de {@link io.mohs.rest.job.JobsControllerContractTest} sobre
 * o {@code @TestConfiguration}/{@code mohs.enabled=false} — mesmo motivo
 * aqui: {@link io.mohs.MohsApplication} exclui {@code io.mohs.rest..*} do
 * component-scan, então controller e {@link RestExceptionHandler} (que
 * converte a {@code UnsupportedOperationException} do stub em 501) entram
 * via {@code @Bean} explícito.
 */
@WebMvcTest(properties = "mohs.enabled=false")
class BatchesControllerContractTest {

    @TestConfiguration
    static class ControllerConfig {

        @Bean
        BatchesController batchesController() {
            return new BatchesController();
        }

        @Bean
        RestExceptionHandler restExceptionHandler() {
            return new RestExceptionHandler();
        }
    }

    @Autowired
    private MockMvc mockMvc;

    @Test
    void getRoutes() throws Exception {
        mockMvc.perform(get(ApiPaths.V1 + "/batches/batch-1")).andExpect(status().isNotImplemented());
    }
}
