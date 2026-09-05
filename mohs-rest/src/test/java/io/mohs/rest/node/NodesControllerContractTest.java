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
package io.mohs.rest.node;

import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import io.mohs.core.EngineState;
import io.mohs.core.Mohs;
import io.mohs.core.NodeSnapshot;
import io.mohs.rest.ApiPaths;
import io.mohs.rest.error.RestExceptionHandler;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** See {@link io.mohs.rest.job.JobsControllerContractTest}'s Javadoc — the same test pattern. */
@WebMvcTest(properties = "mohs.enabled=false")
class NodesControllerContractTest {

    private static final Instant NOW = Instant.parse("2026-08-16T12:00:00Z");

    @TestConfiguration
    static class ControllerConfig {

        @Bean
        NodesController nodesController(Mohs mohs) {
            return new NodesController(mohs);
        }

        @Bean
        RestExceptionHandler restExceptionHandler() {
            return new RestExceptionHandler();
        }
    }

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private Mohs mohs;

    /** The response is the flat form of NodeSnapshot, in the facade's order (most recent first) — the controller neither reorders nor derives anything. */
    @Test
    void listReturnsTheClusterNodesInFacadeOrder() throws Exception {
        when(mohs.nodes()).thenReturn(List.of(
                new NodeSnapshot("node-fresh", EngineState.RUNNING, NOW),
                new NodeSnapshot("node-stopped", EngineState.STOPPED, NOW.minusSeconds(30))));

        mockMvc.perform(get(ApiPaths.V1 + "/nodes"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(2))
                .andExpect(jsonPath("$[0].nodeId").value("node-fresh"))
                .andExpect(jsonPath("$[0].state").value("RUNNING"))
                .andExpect(jsonPath("$[0].lastHeartbeatAt").value("2026-08-16T12:00:00Z"))
                .andExpect(jsonPath("$[1].nodeId").value("node-stopped"))
                .andExpect(jsonPath("$[1].state").value("STOPPED"));
    }

    @Test
    void listIsEmptyWhenNoNodeHasEverBeaten() throws Exception {
        when(mohs.nodes()).thenReturn(List.of());

        mockMvc.perform(get(ApiPaths.V1 + "/nodes"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(0));
    }
}
