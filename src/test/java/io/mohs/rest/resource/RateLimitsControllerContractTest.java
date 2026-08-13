package io.mohs.rest.resource;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.patch;

@WebMvcTest(RateLimitsController.class)
class RateLimitsControllerContractTest {

    @Autowired
    private MockMvc mockMvc;

    @Test
    void listRoutes() {
        assertThatThrownBy(() -> mockMvc.perform(get("/api/mohs/v1/rate-limits")))
                .hasCauseInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void patchRoutes() {
        assertThatThrownBy(() -> mockMvc.perform(patch("/api/mohs/v1/rate-limits/smtp")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"max\":100,\"window\":\"PT1M\"}")))
                .hasCauseInstanceOf(UnsupportedOperationException.class);
    }
}
