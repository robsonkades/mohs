package io.mohs.rest.job;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import io.mohs.rest.ApiPaths;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Contrato de rota (path/verbo/binding), não comportamento — M2 não tem
 * lógica real ainda, então o corpo stub lança {@link UnsupportedOperationException},
 * que o catch-all de {@code RestExceptionHandler} (REST-2) converte em 500;
 * o assert nesse status é a prova de que a rota bateu e chegou lá.
 */
@WebMvcTest(JobsController.class)
class JobsControllerContractTest {

    @Autowired
    private MockMvc mockMvc;

    @Test
    void listRoutes() throws Exception {
        mockMvc.perform(get(ApiPaths.V1 + "/jobs")).andExpect(status().isInternalServerError());
    }

    @Test
    void getRoutes() throws Exception {
        mockMvc.perform(get(ApiPaths.V1 + "/jobs/welcome-email")).andExpect(status().isInternalServerError());
    }

    @Test
    void scheduleRoutes() throws Exception {
        mockMvc.perform(post(ApiPaths.V1 + "/jobs/welcome-email/schedule")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"payload\":{}}"))
                .andExpect(status().isInternalServerError());
    }

    @Test
    void pauseRoutes() throws Exception {
        mockMvc.perform(post(ApiPaths.V1 + "/jobs/welcome-email/pause")).andExpect(status().isInternalServerError());
    }

    @Test
    void resumeRoutes() throws Exception {
        mockMvc.perform(post(ApiPaths.V1 + "/jobs/welcome-email/resume")).andExpect(status().isInternalServerError());
    }

    @Test
    void executionsRoutes() throws Exception {
        mockMvc.perform(get(ApiPaths.V1 + "/jobs/welcome-email/executions")).andExpect(status().isInternalServerError());
    }
}
