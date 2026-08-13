package io.mohs.rest.ratelimit;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import io.mohs.rest.ApiPaths;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.patch;
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
}
