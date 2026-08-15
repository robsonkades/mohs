package io.mohs.rest.ratelimit;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.context.annotation.Bean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import io.mohs.rest.ApiPaths;
import io.mohs.rest.error.RestExceptionHandler;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.patch;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Ver Javadoc de {@link io.mohs.rest.job.JobsControllerContractTest} sobre
 * o {@code @TestConfiguration}/{@code mohs.enabled=false} — mesmo motivo
 * aqui: {@link io.mohs.MohsApplication} exclui {@code io.mohs.rest..*} do
 * component-scan. {@link RestExceptionHandler} entra também: {@link
 * #patchWithInvalidBodySurfacesTheValidationMessageAs422} depende dele.
 */
@WebMvcTest(properties = "mohs.enabled=false")
class RateLimitsControllerContractTest {

    @TestConfiguration
    static class ControllerConfig {

        @Bean
        RateLimitsController rateLimitsController() {
            return new RateLimitsController();
        }

        @Bean
        RestExceptionHandler restExceptionHandler() {
            return new RestExceptionHandler();
        }
    }

    @Autowired
    private MockMvc mockMvc;

    @Test
    void listRoutes() throws Exception {
        mockMvc.perform(get(ApiPaths.V1 + "/rate-limits")).andExpect(status().isNotImplemented());
    }

    @Test
    void patchRoutes() throws Exception {
        mockMvc.perform(patch(ApiPaths.V1 + "/rate-limits/smtp")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"max\":100,\"window\":\"PT1M\"}"))
                .andExpect(status().isNotImplemented());
    }

    /**
     * REST-3 (docs/codereview.md): a validação de {@link RateLimitPatchRequest}
     * roda no compact constructor, disparada durante a desserialização do
     * Jackson — sem {@code io.mohs.rest.error.RestExceptionHandler#handleHttpMessageNotReadable}
     * interceptando, o Spring substituiria a mensagem original por "Failed
     * to read request" e devolveria 400 genérico. Prova que a mensagem real
     * chega ao cliente, com o status 422 que o design REST promete pra
     * payload inválido.
     */
    @Test
    void patchWithInvalidBodySurfacesTheValidationMessageAs422() throws Exception {
        mockMvc.perform(patch(ApiPaths.V1 + "/rate-limits/smtp")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"max\":0,\"window\":\"PT1M\"}"))
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.detail").value("max must be at least 1"));
    }
}
