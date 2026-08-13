package io.mohs.rest.overview;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.test.web.servlet.MockMvc;

import io.mohs.rest.ApiPaths;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** Ver Javadoc de {@link io.mohs.rest.job.JobsControllerContractTest} — mesmo padrão de contrato. */
@WebMvcTest(OverviewController.class)
class OverviewControllerContractTest {

    @Autowired
    private MockMvc mockMvc;

    @Test
    void overviewRoutes() throws Exception {
        mockMvc.perform(get(ApiPaths.V1 + "/overview")).andExpect(status().isInternalServerError());
    }
}
