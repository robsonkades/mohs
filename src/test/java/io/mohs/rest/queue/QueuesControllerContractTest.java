package io.mohs.rest.queue;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.patch;

@WebMvcTest(QueuesController.class)
class QueuesControllerContractTest {

    @Autowired
    private MockMvc mockMvc;

    @Test
    void listRoutes() {
        assertThatThrownBy(() -> mockMvc.perform(get("/api/mohs/v1/queues")))
                .hasCauseInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void patchRoutes() {
        assertThatThrownBy(() -> mockMvc.perform(patch("/api/mohs/v1/queues/emails")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"maxConcurrent\":10}")))
                .hasCauseInstanceOf(UnsupportedOperationException.class);
    }
}
