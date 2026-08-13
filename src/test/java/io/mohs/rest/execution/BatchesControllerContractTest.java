package io.mohs.rest.execution;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.test.web.servlet.MockMvc;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;

@WebMvcTest(BatchesController.class)
class BatchesControllerContractTest {

    @Autowired
    private MockMvc mockMvc;

    @Test
    void getRoutes() {
        assertThatThrownBy(() -> mockMvc.perform(get("/api/mohs/v1/batches/batch-1")))
                .hasCauseInstanceOf(UnsupportedOperationException.class);
    }
}
