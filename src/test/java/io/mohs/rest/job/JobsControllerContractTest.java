package io.mohs.rest.job;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;

/**
 * Contrato de rota (path/verbo/binding), não comportamento — M2 não tem
 * lógica real ainda, então o assert é a causa {@link UnsupportedOperationException}
 * do corpo stub, prova de que a rota bateu e chegou lá.
 */
@WebMvcTest(JobsController.class)
class JobsControllerContractTest {

    @Autowired
    private MockMvc mockMvc;

    @Test
    void listRoutes() {
        assertThatThrownBy(() -> mockMvc.perform(get("/api/mohs/v1/jobs")))
                .hasCauseInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void getRoutes() {
        assertThatThrownBy(() -> mockMvc.perform(get("/api/mohs/v1/jobs/welcome-email")))
                .hasCauseInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void scheduleRoutes() {
        assertThatThrownBy(() -> mockMvc.perform(post("/api/mohs/v1/jobs/welcome-email/schedule")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"payload\":{}}")))
                .hasCauseInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void pauseRoutes() {
        assertThatThrownBy(() -> mockMvc.perform(post("/api/mohs/v1/jobs/welcome-email/pause")))
                .hasCauseInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void resumeRoutes() {
        assertThatThrownBy(() -> mockMvc.perform(post("/api/mohs/v1/jobs/welcome-email/resume")))
                .hasCauseInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void executionsRoutes() {
        assertThatThrownBy(() -> mockMvc.perform(get("/api/mohs/v1/jobs/welcome-email/executions")))
                .hasCauseInstanceOf(UnsupportedOperationException.class);
    }
}
