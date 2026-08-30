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
package io.mohs.rest.job;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.context.annotation.Bean;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import tools.jackson.databind.ObjectMapper;

import io.mohs.core.JobSnapshot;
import io.mohs.core.Mohs;
import io.mohs.core.ScheduleCommand;
import io.mohs.core.definition.JobDefinition;
import io.mohs.core.definition.JobSpec;
import io.mohs.core.event.Enqueued;
import io.mohs.core.execution.Execution;
import io.mohs.core.execution.ExecutionId;
import io.mohs.core.execution.ExecutionState;
import io.mohs.core.execution.Priority;
import io.mohs.core.job.JobKey;
import io.mohs.core.schedule.IntervalSpec;
import io.mohs.rest.ActorResolver;
import io.mohs.rest.ApiPaths;
import io.mohs.rest.error.InvalidActorException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.patch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * The real behaviour of {@link JobsController} over a mocked {@link Mohs} — {@code mohs.enabled=false}
 * turns off {@code MohsAutoConfiguration} (which would demand a {@code DataSource}) wherever it is
 * on the classpath.
 *
 * <p>{@code JobsController} arrives through an explicit {@code @Bean} rather than
 * {@code @WebMvcTest(controllers = ...)}: the slice inherits the component scan of the nearest
 * {@code @SpringBootConfiguration}, and this module's ({@link io.mohs.rest.RestSliceConfiguration})
 * does not scan {@code io.mohs.rest} — two bean definitions for the same controller would be the
 * result, and the exclusion would block the explicit {@code controllers = JobsController.class}
 * inclusion as well.
 */
@WebMvcTest(properties = "mohs.enabled=false")
class JobsControllerContractTest {

    private static final Instant NOW = Instant.parse("2026-08-15T12:00:00Z");

    record Handler() {
    }

    @TestConfiguration
    static class ControllerConfig {

        @Bean
        JobsController jobsController(Mohs mohs, ActorResolver actorResolver, ObjectMapper objectMapper) {
            return new JobsController(mohs, actorResolver, objectMapper, ApiPaths.V1);
        }

        @Bean
        io.mohs.rest.error.RestExceptionHandler restExceptionHandler() {
            return new io.mohs.rest.error.RestExceptionHandler();
        }
    }

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private Mohs mohs;

    @MockitoBean
    private ActorResolver actorResolver;

    private static JobSnapshot snapshot(String key, boolean paused) {
        JobDefinition definition = JobDefinition.of(key, Handler.class, JobSpec::onDemand);
        return new JobSnapshot(definition, paused, null);
    }

    @Test
    void listReturnsRegisteredJobs() throws Exception {
        when(mohs.jobs()).thenReturn(List.of(snapshot("welcome-email", false)));

        mockMvc.perform(get(ApiPaths.V1 + "/jobs"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].jobKey").value("welcome-email"));
    }

    @Test
    void getUnknownJobReturns404WithNearbySuggestion() throws Exception {
        when(mohs.findJob(JobKey.of("wellcome-email"))).thenReturn(Optional.empty());
        when(mohs.jobs()).thenReturn(List.of(snapshot("welcome-email", false)));

        mockMvc.perform(get(ApiPaths.V1 + "/jobs/wellcome-email"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.nearbyJobKeys[0]").value("welcome-email"));
    }

    @Test
    void scheduleAcceptsAndPointsLocationAtTheExecution() throws Exception {
        JobKey key = JobKey.of("welcome-email");
        when(mohs.findJob(key)).thenReturn(Optional.of(snapshot("welcome-email", false)));
        when(mohs.payloadType(key)).thenReturn(Optional.empty());
        when(actorResolver.resolve(any())).thenReturn("tester");
        ScheduleCommand command = mock(ScheduleCommand.class);
        when(mohs.schedule(eq("welcome-email"), any())).thenReturn(command);
        when(command.as("tester")).thenReturn(command);
        when(command.now()).thenReturn(new Enqueued(ExecutionId.of("exec-1"), key, NOW, "tester"));

        mockMvc.perform(post(ApiPaths.V1 + "/jobs/welcome-email/schedule")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"payload\":{}}"))
                .andExpect(status().isAccepted())
                .andExpect(header().string("Location", ApiPaths.V1 + "/executions/exec-1"))
                .andExpect(jsonPath("$.executionId").value("exec-1"));
    }

    /** A complete invocation: delay (computed on the server) and priority reach the ScheduleCommand — the core's three terminals at the REST boundary. */
    @Test
    void scheduleAcceptsDelayAndPriority() throws Exception {
        JobKey key = JobKey.of("welcome-email");
        when(mohs.findJob(key)).thenReturn(Optional.of(snapshot("welcome-email", false)));
        when(mohs.payloadType(key)).thenReturn(Optional.empty());
        when(actorResolver.resolve(any())).thenReturn("tester");
        ScheduleCommand command = mock(ScheduleCommand.class);
        when(mohs.schedule(eq("welcome-email"), any())).thenReturn(command);
        when(command.as("tester")).thenReturn(command);
        when(command.priority(Priority.HIGH)).thenReturn(command);
        when(command.after(Duration.ofMinutes(5))).thenReturn(new Enqueued(ExecutionId.of("exec-2"), key, NOW.plusSeconds(300), "tester"));

        mockMvc.perform(post(ApiPaths.V1 + "/jobs/welcome-email/schedule")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"payload\":{},\"delay\":\"PT5M\",\"priority\":\"HIGH\"}"))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.executionId").value("exec-2"));
    }

    /** at and delay are exclusive — the record's validation fires during deserialisation and becomes a 422 that teaches. */
    @Test
    void scheduleRejectsAtAndDelayTogether() throws Exception {
        JobKey key = JobKey.of("welcome-email");
        when(mohs.findJob(key)).thenReturn(Optional.of(snapshot("welcome-email", false)));

        mockMvc.perform(post(ApiPaths.V1 + "/jobs/welcome-email/schedule")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"payload\":{},\"at\":\"2026-08-15T13:00:00Z\",\"delay\":\"PT5M\"}"))
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.detail").value(containsString("mutually exclusive")));
    }

    /** A schedule PATCH returns the emergency envelope carrying the warning about reversion at boot. */
    @Test
    void reschedulePatchesTheScheduleUnderTheEmergencyContract() throws Exception {
        JobKey key = JobKey.of("welcome-email");
        when(mohs.findJob(key)).thenReturn(Optional.of(snapshot("welcome-email", false)));
        when(actorResolver.resolve(any())).thenReturn("ana.ops");
        when(mohs.reschedule(key, new IntervalSpec(Duration.ofSeconds(30), false)))
                .thenReturn(Optional.of(snapshot("welcome-email", false)));

        mockMvc.perform(patch(ApiPaths.V1 + "/jobs/welcome-email/schedule")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"type\":\"INTERVAL\",\"interval\":\"PT30S\",\"afterFinish\":false}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.resource.jobKey").value("welcome-email"))
                .andExpect(jsonPath("$.notice").value(containsString("boot")));
    }

    @Test
    void rescheduleOfUnknownJobReturns404() throws Exception {
        when(mohs.findJob(JobKey.of("ghost"))).thenReturn(Optional.empty());
        when(mohs.jobs()).thenReturn(List.of());

        mockMvc.perform(patch(ApiPaths.V1 + "/jobs/ghost/schedule")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"type\":\"ON_DEMAND\"}"))
                .andExpect(status().isNotFound());
    }

    /** An invalid actor is rejected BEFORE any effect — a 4xx is a contract of "nothing changed". */
    @Test
    void rescheduleWithAnInvalidActorMutatesNothing() throws Exception {
        JobKey key = JobKey.of("welcome-email");
        when(mohs.findJob(key)).thenReturn(Optional.of(snapshot("welcome-email", false)));
        when(actorResolver.resolve(any())).thenThrow(new InvalidActorException("X-Mohs-Actor must not be 'scheduler'"));

        mockMvc.perform(patch(ApiPaths.V1 + "/jobs/welcome-email/schedule")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"type\":\"ON_DEMAND\"}"))
                .andExpect(status().isBadRequest());

        verify(mohs, never()).reschedule(any(), any());
    }

    /** An IAE from the spec's compact constructor (a non-positive interval) is also a 422 with field "schedule" — never a 500. */
    @Test
    void rescheduleRejectsANonPositiveInterval() throws Exception {
        JobKey key = JobKey.of("welcome-email");
        when(mohs.findJob(key)).thenReturn(Optional.of(snapshot("welcome-email", false)));
        when(actorResolver.resolve(any())).thenReturn("ana.ops");

        mockMvc.perform(patch(ApiPaths.V1 + "/jobs/welcome-email/schedule")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"type\":\"INTERVAL\",\"interval\":\"PT0S\",\"afterFinish\":false}"))
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.field").value("schedule"))
                .andExpect(jsonPath("$.detail").value(containsString("positive")));
    }

    /** An unrealisable schedule (a cron that never fires) is a 422 that teaches — the trigger calculation's IAE never becomes a 500. */
    @Test
    void rescheduleRejectsAnImpossibleCron() throws Exception {
        JobKey key = JobKey.of("welcome-email");
        when(mohs.findJob(key)).thenReturn(Optional.of(snapshot("welcome-email", false)));
        when(mohs.reschedule(eq(key), any()))
                .thenThrow(new IllegalArgumentException("Cron expression never fires within the search bound: 0 0 0 30 2 *"));

        mockMvc.perform(patch(ApiPaths.V1 + "/jobs/welcome-email/schedule")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"type\":\"CRON\",\"expression\":\"0 0 0 30 2 *\",\"zone\":\"UTC\"}"))
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.field").value("schedule"))
                .andExpect(jsonPath("$.detail").value(containsString("never fires")));
    }

    @Test
    void scheduleRejectsAPayloadTheHandlerDoesNotAccept() throws Exception {
        JobKey key = JobKey.of("welcome-email");
        when(mohs.findJob(key)).thenReturn(Optional.of(snapshot("welcome-email", false)));
        when(mohs.payloadType(key)).thenReturn(Optional.empty());

        mockMvc.perform(post(ApiPaths.V1 + "/jobs/welcome-email/schedule")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"payload\":{\"user\":\"ana\"}}"))
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.field").value("payload"));
    }

    record CountPayload(int count) {
    }

    /** The convertValue branch: in Jackson 3 a databind failure is NOT an IllegalArgumentException — without the right catch this was a 500, not the contract's 422. */
    @Test
    void scheduleRejectsAPayloadIncompatibleWithTheDeclaredTypeAs422() throws Exception {
        JobKey key = JobKey.of("welcome-email");
        when(mohs.findJob(key)).thenReturn(Optional.of(snapshot("welcome-email", false)));
        when(mohs.payloadType(key)).thenReturn(Optional.<Class<?>>of(CountPayload.class));

        mockMvc.perform(post(ApiPaths.V1 + "/jobs/welcome-email/schedule")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"payload\":{\"count\":\"not-a-number\"}}"))
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.field").value("payload"))
                .andExpect(result -> {
                    String body = result.getResponse().getContentAsString();
                    assertThat(body).contains("CountPayload");
                });
    }

    @Test
    void pauseReflectsThePausedStateInTheResponse() throws Exception {
        JobKey key = JobKey.of("welcome-email");
        when(mohs.findJob(key)).thenReturn(Optional.of(snapshot("welcome-email", false)), Optional.of(snapshot("welcome-email", true)));

        mockMvc.perform(post(ApiPaths.V1 + "/jobs/welcome-email/pause"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.paused").value(true));
    }

    @Test
    void resumeReflectsTheResumedStateInTheResponse() throws Exception {
        JobKey key = JobKey.of("welcome-email");
        when(mohs.findJob(key)).thenReturn(Optional.of(snapshot("welcome-email", true)), Optional.of(snapshot("welcome-email", false)));

        mockMvc.perform(post(ApiPaths.V1 + "/jobs/welcome-email/resume"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.paused").value(false));
    }

    @Test
    void executionsSignalsANextPageViaCursorWhenThereAreMoreRows() throws Exception {
        JobKey key = JobKey.of("welcome-email");
        when(mohs.findJob(key)).thenReturn(Optional.of(snapshot("welcome-email", false)));
        Execution first = new Execution(ExecutionId.of("exec-2"), key, ExecutionState.ENQUEUED, NOW, null, List.of(), "tester");
        Execution second = new Execution(ExecutionId.of("exec-1"), key, ExecutionState.ENQUEUED, NOW.minusSeconds(1), null, List.of(), "tester");
        when(mohs.executions(any())).thenReturn(List.of(first, second));

        mockMvc.perform(get(ApiPaths.V1 + "/jobs/welcome-email/executions").param("size", "1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.items.length()").value(1))
                .andExpect(jsonPath("$.items[0].executionId").value("exec-2"))
                .andExpect(jsonPath("$.nextCursor").value("exec-2"));
    }

    /** size=0 and size=-1 saturate at 1 (CursorPage.clampSize) — previously 0 threw IndexOutOfBounds and a negative threw IAE, both 500s. */
    @Test
    void executionsSaturatesANonPositiveSizeInsteadOfFailing() throws Exception {
        JobKey key = JobKey.of("welcome-email");
        when(mohs.findJob(key)).thenReturn(Optional.of(snapshot("welcome-email", false)));
        Execution first = new Execution(ExecutionId.of("exec-2"), key, ExecutionState.ENQUEUED, NOW, null, List.of(), "tester");
        Execution second = new Execution(ExecutionId.of("exec-1"), key, ExecutionState.ENQUEUED, NOW.minusSeconds(1), null, List.of(), "tester");
        when(mohs.executions(any())).thenReturn(List.of(first, second));

        mockMvc.perform(get(ApiPaths.V1 + "/jobs/welcome-email/executions").param("size", "0"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.items.length()").value(1));

        mockMvc.perform(get(ApiPaths.V1 + "/jobs/welcome-email/executions").param("size", "-1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.items.length()").value(1));
    }
}
