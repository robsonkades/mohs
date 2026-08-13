package io.mohs.rest.resource;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.test.web.servlet.MockMvc;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;

@WebMvcTest(RunnersController.class)
class RunnersControllerContractTest {

    @Autowired
    private MockMvc mockMvc;

    @Test
    void listRoutes() {
        assertThatThrownBy(() -> mockMvc.perform(get("/api/mohs/v1/runners")))
                .hasCauseInstanceOf(UnsupportedOperationException.class);
    }
}
