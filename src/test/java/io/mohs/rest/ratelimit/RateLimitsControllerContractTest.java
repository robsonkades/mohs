package io.mohs.rest.ratelimit;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import io.mohs.rest.ApiPaths;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.patch;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** Ver Javadoc de {@link io.mohs.rest.job.JobsControllerContractTest} — mesmo padrão de contrato. */
@WebMvcTest(RateLimitsController.class)
class RateLimitsControllerContractTest {

    @Autowired
    private MockMvc mockMvc;

    @Test
    void listRoutes() throws Exception {
        mockMvc.perform(get(ApiPaths.V1 + "/rate-limits")).andExpect(status().isInternalServerError());
    }

    @Test
    void patchRoutes() throws Exception {
        mockMvc.perform(patch(ApiPaths.V1 + "/rate-limits/smtp")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"max\":100,\"window\":\"PT1M\"}"))
                .andExpect(status().isInternalServerError());
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
