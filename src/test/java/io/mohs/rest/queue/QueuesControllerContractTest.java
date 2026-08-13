package io.mohs.rest.queue;

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
@WebMvcTest(QueuesController.class)
class QueuesControllerContractTest {

    @Autowired
    private MockMvc mockMvc;

    @Test
    void listRoutes() throws Exception {
        mockMvc.perform(get(ApiPaths.V1 + "/queues")).andExpect(status().isInternalServerError());
    }

    @Test
    void patchRoutes() throws Exception {
        mockMvc.perform(patch(ApiPaths.V1 + "/queues/emails")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"maxConcurrent\":10}"))
                .andExpect(status().isInternalServerError());
    }
}
