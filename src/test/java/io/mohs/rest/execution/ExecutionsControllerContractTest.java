package io.mohs.rest.execution;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.test.web.servlet.MockMvc;

import io.mohs.rest.ApiPaths;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** Ver Javadoc de {@link io.mohs.rest.job.JobsControllerContractTest} — mesmo padrão de contrato. */
@WebMvcTest(ExecutionsController.class)
class ExecutionsControllerContractTest {

    @Autowired
    private MockMvc mockMvc;

    @Test
    void searchRoutes() throws Exception {
        mockMvc.perform(get(ApiPaths.V1 + "/executions")
                        .param("status", "RUNNING").param("jobKey", "welcome-email"))
                .andExpect(status().isInternalServerError());
    }

    @Test
    void getRoutes() throws Exception {
        mockMvc.perform(get(ApiPaths.V1 + "/executions/exec-1")).andExpect(status().isInternalServerError());
    }

    @Test
    void cancelRoutes() throws Exception {
        mockMvc.perform(post(ApiPaths.V1 + "/executions/exec-1/cancel")).andExpect(status().isInternalServerError());
    }

    @Test
    void retryRoutes() throws Exception {
        mockMvc.perform(post(ApiPaths.V1 + "/executions/exec-1/retry")).andExpect(status().isInternalServerError());
    }
}
