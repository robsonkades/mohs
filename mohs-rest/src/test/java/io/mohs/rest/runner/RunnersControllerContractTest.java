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
package io.mohs.rest.runner;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.context.annotation.Bean;
import org.springframework.test.web.servlet.MockMvc;

import io.mohs.core.Mohs;
import io.mohs.core.RunnerSnapshot;
import io.mohs.core.resource.RunnerMode;
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
class RunnersControllerContractTest {

    private static final Mohs MOHS = mock(Mohs.class);

    @TestConfiguration
    static class ControllerConfig {

        @Bean
        RunnersController runnersController() {
            return new RunnersController(MOHS);
        }

        @Bean
        RestExceptionHandler restExceptionHandler() {
            return new RestExceptionHandler();
        }
    }

    @Autowired
    private MockMvc mockMvc;

    /** The design document's contract: name, mode, ceiling and occupancy — no cursor, since the list is what the boot declared. */
    @Test
    void listExposesModeMaxAndRunningPerRunner() throws Exception {
        when(MOHS.runners()).thenReturn(List.of(
                new RunnerSnapshot("cpu-heavy", RunnerMode.CPU, 8, 3),
                new RunnerSnapshot("io", RunnerMode.IO, 200, 17)));

        mockMvc.perform(get(ApiPaths.V1 + "/runners"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(2))
                .andExpect(jsonPath("$[0].name").value("cpu-heavy"))
                .andExpect(jsonPath("$[0].mode").value("CPU"))
                .andExpect(jsonPath("$[0].max").value(8))
                .andExpect(jsonPath("$[0].running").value(3))
                .andExpect(jsonPath("$[1].name").value("io"))
                .andExpect(jsonPath("$[1].mode").value("IO"));
    }

    /** A node with no declared runner is impossible (the registry requires the default), but an empty list must not become a 500. */
    @Test
    void listOfNoRunnersIsAnEmptyArray() throws Exception {
        when(MOHS.runners()).thenReturn(List.of());

        mockMvc.perform(get(ApiPaths.V1 + "/runners"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(0));
    }
}
