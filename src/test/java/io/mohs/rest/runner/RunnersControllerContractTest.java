package io.mohs.rest.runner;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.test.web.servlet.MockMvc;

import io.mohs.rest.ApiPaths;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** Ver Javadoc de {@link io.mohs.rest.job.JobsControllerContractTest} — mesmo padrão de contrato. */
@WebMvcTest(RunnersController.class)
class RunnersControllerContractTest {

    @Autowired
    private MockMvc mockMvc;

    @Test
    void listRoutes() throws Exception {
        mockMvc.perform(get(ApiPaths.V1 + "/runners")).andExpect(status().isInternalServerError());
    }
}
