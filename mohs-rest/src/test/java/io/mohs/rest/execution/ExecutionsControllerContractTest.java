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
package io.mohs.rest.execution;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import io.mohs.core.Mohs;
import io.mohs.core.execution.Execution;
import io.mohs.core.execution.ExecutionId;
import io.mohs.core.execution.ExecutionState;
import io.mohs.core.job.JobKey;
import io.mohs.rest.ActorResolver;
import io.mohs.rest.ApiPaths;
import io.mohs.rest.error.InvalidActorException;
import io.mohs.rest.error.RestExceptionHandler;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** See {@link io.mohs.rest.job.JobsControllerContractTest}'s Javadoc — the same test pattern. */
@WebMvcTest(properties = "mohs.enabled=false")
class ExecutionsControllerContractTest {

    private static final Instant NOW = Instant.parse("2026-08-15T12:00:00Z");

    @TestConfiguration
    static class ControllerConfig {

        @Bean
        ExecutionsController executionsController(Mohs mohs, ActorResolver actorResolver) {
            return new ExecutionsController(mohs, actorResolver);
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

    @MockitoBean
    private ActorResolver actorResolver;

    /** An invalid actor is rejected BEFORE any effect — a 4xx is a contract of "nothing changed", on cancel as on every mutation. */
    @Test
    void cancelWithAnInvalidActorMutatesNothing() throws Exception {
        when(actorResolver.resolve(any())).thenThrow(new InvalidActorException("X-Mohs-Actor must not be 'scheduler'"));

        mockMvc.perform(post(ApiPaths.V1 + "/executions/exec-1/cancel").header("X-Mohs-Actor", "scheduler"))
                .andExpect(status().isBadRequest());

        verify(mohs, never()).cancel(any());
    }

    @Test
    void retryWithAnInvalidActorMutatesNothing() throws Exception {
        when(actorResolver.resolve(any())).thenThrow(new InvalidActorException("X-Mohs-Actor must not be 'scheduler'"));

        mockMvc.perform(post(ApiPaths.V1 + "/executions/exec-1/retry").header("X-Mohs-Actor", "scheduler"))
                .andExpect(status().isBadRequest());

        verify(mohs, never()).retry(any());
    }

    @Test
    void searchSignalsANextPageViaCursorWhenThereAreMoreRows() throws Exception {
        JobKey key = JobKey.of("welcome-email");
        Execution first = new Execution(ExecutionId.of("exec-2"), key, ExecutionState.ENQUEUED, NOW, null, List.of(), "tester");
        Execution second = new Execution(ExecutionId.of("exec-1"), key, ExecutionState.ENQUEUED, NOW.minusSeconds(1), null, List.of(), "tester");
        when(mohs.executions(any())).thenReturn(List.of(first, second));

        mockMvc.perform(get(ApiPaths.V1 + "/executions")
                        .param("status", "ENQUEUED").param("jobKey", "welcome-email").param("size", "1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.items.length()").value(1))
                .andExpect(jsonPath("$.items[0].executionId").value("exec-2"))
                // The list is a summary: attempts live in the detail view, GET /executions/{id}
                .andExpect(jsonPath("$.items[0].attempts").doesNotExist())
                .andExpect(jsonPath("$.nextCursor").value("exec-2"));
    }

    /** size=0 and size=-1 saturate at 1 (CursorPage.clampSize) — previously 0 threw IndexOutOfBounds and a negative threw IAE, both 500s. */
    @Test
    void searchSaturatesANonPositiveSizeInsteadOfFailing() throws Exception {
        JobKey key = JobKey.of("welcome-email");
        Execution first = new Execution(ExecutionId.of("exec-2"), key, ExecutionState.ENQUEUED, NOW, null, List.of(), "tester");
        Execution second = new Execution(ExecutionId.of("exec-1"), key, ExecutionState.ENQUEUED, NOW.minusSeconds(1), null, List.of(), "tester");
        when(mohs.executions(any())).thenReturn(List.of(first, second));

        mockMvc.perform(get(ApiPaths.V1 + "/executions").param("size", "0"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.items.length()").value(1));

        mockMvc.perform(get(ApiPaths.V1 + "/executions").param("size", "-1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.items.length()").value(1));
    }

    @Test
    void getReturnsTheExecution() throws Exception {
        JobKey key = JobKey.of("welcome-email");
        Execution execution = new Execution(ExecutionId.of("exec-1"), key, ExecutionState.SUCCEEDED, NOW, NOW, List.of(), "tester");
        when(mohs.findExecution(ExecutionId.of("exec-1"))).thenReturn(Optional.of(execution));

        mockMvc.perform(get(ApiPaths.V1 + "/executions/exec-1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.executionId").value("exec-1"))
                .andExpect(jsonPath("$.state").value("SUCCEEDED"));
    }

    /** A whitespace-only id used to reach {@code ExecutionId.of} and come back as a 500 with a stack trace in the log — reachable by anyone, on every route that takes an id. */
    @Test
    void aBlankIdIsAValidationErrorNotA500() throws Exception {
        mockMvc.perform(get(ApiPaths.V1 + "/executions/{id}", " "))
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.field").value("id"));
        mockMvc.perform(post(ApiPaths.V1 + "/executions/{id}/cancel", " "))
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.field").value("id"));
    }

    /** A blank filter is no filter: {@code ?jobKey=} lists everything instead of failing on an empty key. */
    @Test
    void aBlankJobKeyFilterIsIgnored() throws Exception {
        when(mohs.executions(any())).thenReturn(List.of());

        mockMvc.perform(get(ApiPaths.V1 + "/executions").param("jobKey", ""))
                .andExpect(status().isOk());

        verify(mohs).executions(argThat(query -> query.jobKey() == null));
    }

    @Test
    void getUnknownExecutionReturns404() throws Exception {
        when(mohs.findExecution(ExecutionId.of("ghost"))).thenReturn(Optional.empty());

        mockMvc.perform(get(ApiPaths.V1 + "/executions/ghost"))
                .andExpect(status().isNotFound());
    }

    /** A 202 with the current state (not necessarily terminal) and a Location pointing at the detail — a cancel is accepted, never guaranteed. */
    @Test
    void cancelReturns202WithTheCurrentStateAndALocationHeader() throws Exception {
        JobKey key = JobKey.of("welcome-email");
        Execution running = new Execution(ExecutionId.of("exec-1"), key, ExecutionState.RUNNING, NOW, NOW, List.of(), "tester");
        when(mohs.cancel(ExecutionId.of("exec-1"))).thenReturn(Optional.of(running));

        mockMvc.perform(post(ApiPaths.V1 + "/executions/exec-1/cancel"))
                .andExpect(status().isAccepted())
                .andExpect(header().string("Location", ApiPaths.V1 + "/executions/exec-1"))
                .andExpect(jsonPath("$.executionId").value("exec-1"))
                .andExpect(jsonPath("$.state").value("RUNNING"));
    }

    @Test
    void cancelOfAnUnknownExecutionReturns404() throws Exception {
        when(mohs.cancel(ExecutionId.of("ghost"))).thenReturn(Optional.empty());

        mockMvc.perform(post(ApiPaths.V1 + "/executions/ghost/cancel"))
                .andExpect(status().isNotFound());
    }

    /** A manual retry: a 202 with the receipt of the SAME rearmed execution (the original actor) and a Location pointing at the detail. */
    @Test
    void retryReturns202WithTheRearmedExecutionAndALocationHeader() throws Exception {
        JobKey key = JobKey.of("welcome-email");
        Execution rearmed = new Execution(ExecutionId.of("exec-1"), key, ExecutionState.RETRY_WAITING, NOW, null, List.of(), "tester");
        when(mohs.retry(ExecutionId.of("exec-1"))).thenReturn(Optional.of(rearmed));

        mockMvc.perform(post(ApiPaths.V1 + "/executions/exec-1/retry"))
                .andExpect(status().isAccepted())
                .andExpect(header().string("Location", ApiPaths.V1 + "/executions/exec-1"))
                .andExpect(jsonPath("$.executionId").value("exec-1"))
                .andExpect(jsonPath("$.jobKey").value("welcome-email"))
                .andExpect(jsonPath("$.actor").value("tester"));
    }

    @Test
    void retryOfAnUnknownExecutionReturns404() throws Exception {
        when(mohs.retry(ExecutionId.of("ghost"))).thenReturn(Optional.empty());

        mockMvc.perform(post(ApiPaths.V1 + "/executions/ghost/retry"))
                .andExpect(status().isNotFound());
    }

    /** A state other than FAILED becomes a 409 that teaches — the facade's state-guard ISE never escapes as a 500. */
    @Test
    void retryOfANonFailedExecutionReturns409NamingTheState() throws Exception {
        when(mohs.retry(ExecutionId.of("exec-1")))
                .thenThrow(new IllegalStateException("execution exec-1 is RUNNING — only FAILED executions can be manually retried"));

        mockMvc.perform(post(ApiPaths.V1 + "/executions/exec-1/retry"))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.title").value("Execution not retryable"))
                .andExpect(jsonPath("$.detail").value(containsString("RUNNING")));
    }
}
