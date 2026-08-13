package io.mohs.rest.execution;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.test.web.servlet.MockMvc;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;

@WebMvcTest(ExecutionsController.class)
class ExecutionsControllerContractTest {

    @Autowired
    private MockMvc mockMvc;

    @Test
    void searchRoutes() {
        assertThatThrownBy(() -> mockMvc.perform(get("/api/mohs/v1/executions")
                        .param("status", "RUNNING").param("jobKey", "welcome-email")))
                .hasCauseInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void getRoutes() {
        assertThatThrownBy(() -> mockMvc.perform(get("/api/mohs/v1/executions/exec-1")))
                .hasCauseInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void cancelRoutes() {
        assertThatThrownBy(() -> mockMvc.perform(post("/api/mohs/v1/executions/exec-1/cancel")))
                .hasCauseInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void retryRoutes() {
        assertThatThrownBy(() -> mockMvc.perform(post("/api/mohs/v1/executions/exec-1/retry")))
                .hasCauseInstanceOf(UnsupportedOperationException.class);
    }
}
