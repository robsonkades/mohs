/*
 * Copyright 2026 The Mohs Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.mohs.rest.batch;

import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.context.annotation.Bean;
import org.springframework.test.web.servlet.MockMvc;

import io.mohs.core.BatchSnapshot;
import io.mohs.core.Mohs;
import io.mohs.rest.ApiPaths;
import io.mohs.rest.error.RestExceptionHandler;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * See {@link io.mohs.rest.job.JobsControllerContractTest}'s Javadoc about the
 * {@code @TestConfiguration}/{@code mohs.enabled=false} pair — the same reason applies here:
 * {@link io.mohs.rest.RestSliceConfiguration} does not component-scan {@code io.mohs.rest}, so the
 * controller and {@link RestExceptionHandler} arrive through an explicit {@code @Bean}.
 */
@WebMvcTest(properties = "mohs.enabled=false")
class BatchesControllerContractTest {

    @TestConfiguration
    static class ControllerConfig {

        @Bean
        Mohs mohs() {
            Mohs mohs = mock(Mohs.class);
            when(mohs.findBatch("batch-1")).thenReturn(Optional.of(new BatchSnapshot("batch-1", "nightly invoices", 10, 7, 1)));
            when(mohs.findBatch("ghost")).thenReturn(Optional.empty());
            return mohs;
        }

        @Bean
        BatchesController batchesController(Mohs mohs) {
            return new BatchesController(mohs);
        }

        @Bean
        RestExceptionHandler restExceptionHandler() {
            return new RestExceptionHandler();
        }
    }

    @Autowired
    private MockMvc mockMvc;

    /**
     * {@code pending} and {@code state} do not come from the database: they are derived from the
     * three counters, so the response cannot contradict itself — 10 total with 8 terminated are 2
     * pending by construction, and the batch is only {@code COMPLETED} when that remainder is zero.
     */
    @Test
    void getReturnsTheCountersWithPendingAndStateDerived() throws Exception {
        mockMvc.perform(get(ApiPaths.V1 + "/batches/batch-1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.batchId").value("batch-1"))
                .andExpect(jsonPath("$.total").value(10))
                .andExpect(jsonPath("$.succeeded").value(7))
                .andExpect(jsonPath("$.failed").value(1))
                .andExpect(jsonPath("$.pending").value(2))
                .andExpect(jsonPath("$.state").value("RUNNING"));
    }

    @Test
    void getAnswers404ForAnUnknownBatch() throws Exception {
        mockMvc.perform(get(ApiPaths.V1 + "/batches/ghost"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.title").value("Batch not found"));
    }
}
