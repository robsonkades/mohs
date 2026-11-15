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

import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.json.JsonMapper;

import io.mohs.core.JobSnapshot;
import io.mohs.core.Mohs;
import io.mohs.core.SubmissionOptions;
import io.mohs.core.NoInput;
import io.mohs.core.job.JobRef;
import io.mohs.core.definition.JobDefinition;
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
        JobsController jobsController(Mohs mohs, ActorResolver actorResolver) {
            // Mirrors InputCodec.JSON's format: the raw mapper refusing unknown properties,
            // never the slice's auto-configured one
            return new JobsController(mohs, actorResolver,
                    JsonMapper.builder().enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES).build(), ApiPaths.V1);
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

    @org.junit.jupiter.api.BeforeEach
    void actorIsResolvedForEveryValidSubmission() {
        when(actorResolver.resolve(any())).thenReturn("tester");
    }

    private static JobSnapshot snapshot(String key, boolean paused) {
        JobDefinition definition = JobDefinition.of(key, new io.mohs.core.schedule.OnDemandSpec(), io.mohs.core.definition.JobOptions.defaults(), io.mohs.core.schedule.Misfire.IGNORE, false);
        return new JobSnapshot(definition, paused, null);
    }

    @Test
    void pauseAndResumeRejectAnInvalidActorBeforeChangingState() throws Exception {
        when(mohs.findJob(JobKey.of("welcome-email"))).thenReturn(Optional.of(snapshot("welcome-email", false)));
        when(actorResolver.resolve(any())).thenThrow(new InvalidActorException("invalid actor"));
        for (String action : List.of("pause", "resume")) {
            mockMvc.perform(post(ApiPaths.V1 + "/jobs/welcome-email/" + action))
                    .andExpect(status().isBadRequest());
        }
        verify(mohs, never()).pause(any());
        verify(mohs, never()).resume(any());
    }

    @Test
    void aResponseReadTimeoutDoesNotClaimThatTheCommittedPauseWasRolledBack() throws Exception {
        when(mohs.findJob(JobKey.of("welcome-email")))
                .thenReturn(Optional.of(snapshot("welcome-email", false)))
                .thenThrow(new org.springframework.dao.QueryTimeoutException("response read timed out"));

        mockMvc.perform(post(ApiPaths.V1 + "/jobs/welcome-email/pause"))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.detail").value(containsString("may already have applied")));
        verify(mohs).pause(JobKey.of("welcome-email"));
    }

    @Test
    void listReturnsRegisteredJobs() throws Exception {
        when(mohs.jobs()).thenReturn(List.of(snapshot("welcome-email", false)));

        mockMvc.perform(get(ApiPaths.V1 + "/jobs"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].jobKey").value("welcome-email"));
    }

    /** A whitespace-only key used to reach {@code JobKey.of} and come back as a 500 with a stack trace in the log — reachable by anyone, so an error alarm on demand. */
    @Test
    void getWithABlankJobKeyIsAValidationErrorNotA500() throws Exception {
        mockMvc.perform(get(ApiPaths.V1 + "/jobs/{jobKey}", " "))
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.field").value("jobKey"))
                .andExpect(jsonPath("$.detail").value(containsString("spaces are not allowed")));
    }

    /** The ceiling is the command's (the column is 255 wide on every dialect); the controller only turns its refusal into the 422 the validation model promises. */
    @Test
    void scheduleRejectsAnIdempotencyKeyWiderThanTheColumn() throws Exception {
        JobKey key = JobKey.of("welcome-email");
        when(mohs.findJob(key)).thenReturn(Optional.of(snapshot("welcome-email", false)));
        when(mohs.jobRef(key)).thenReturn(Optional.of(JobRef.of(key.value(), NoInput.class)));
        when(actorResolver.resolve(any())).thenReturn("tester");

        mockMvc.perform(post(ApiPaths.V1 + "/jobs/welcome-email/schedule")
                        .contentType(MediaType.APPLICATION_JSON)
                        .header("Idempotency-Key", "k".repeat(256))
                        .content("{\"payload\":{}}"))
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.field").value("Idempotency-Key"))
                .andExpect(jsonPath("$.detail").value(containsString("255")));
        verify(mohs, never()).enqueue(any(), any(), any());
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
        when(mohs.jobRef(key)).thenReturn(Optional.of(JobRef.of(key.value(), NoInput.class)));
        when(actorResolver.resolve(any())).thenReturn("tester");
        when(mohs.enqueue(any(), any(), eq(SubmissionOptions.defaults().withActor("tester")))).thenReturn(new Enqueued(ExecutionId.of("01991337-0000-7000-8000-00000000001c"), key, NOW, "tester"));

        mockMvc.perform(post(ApiPaths.V1 + "/jobs/welcome-email/schedule")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"payload\":{}}"))
                .andExpect(status().isAccepted())
                .andExpect(header().string("Location", ApiPaths.V1 + "/executions/01991337-0000-7000-8000-00000000001c"))
                .andExpect(jsonPath("$.executionId").value("01991337-0000-7000-8000-00000000001c"));
    }

    /** A complete invocation: delay (computed on the server) and priority reach the ScheduleCommand — the core's three terminals at the REST boundary. */
    @Test
    void scheduleAcceptsDelayAndPriority() throws Exception {
        JobKey key = JobKey.of("welcome-email");
        when(mohs.findJob(key)).thenReturn(Optional.of(snapshot("welcome-email", false)));
        when(mohs.jobRef(key)).thenReturn(Optional.of(JobRef.of(key.value(), NoInput.class)));
        when(actorResolver.resolve(any())).thenReturn("tester");
        when(mohs.scheduleAfter(any(), any(), eq(Duration.ofMinutes(5)), eq(SubmissionOptions.defaults().withActor("tester").withPriority(Priority.HIGH)))).thenReturn(new Enqueued(ExecutionId.of("01991337-0000-7000-8000-00000000001e"), key, NOW.plusSeconds(300), "tester"));

        mockMvc.perform(post(ApiPaths.V1 + "/jobs/welcome-email/schedule")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"payload\":{},\"delay\":\"PT5M\",\"priority\":\"HIGH\"}"))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.executionId").value("01991337-0000-7000-8000-00000000001e"));
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
        when(mohs.jobRef(key)).thenReturn(Optional.of(JobRef.of(key.value(), NoInput.class)));

        mockMvc.perform(post(ApiPaths.V1 + "/jobs/welcome-email/schedule")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"payload\":{\"user\":\"ana\"}}"))
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.field").value("payload"));
    }

    record CountPayload(int count) {
    }

    /** A misspelt field is refused, never a silent null the job runs with — the strict mapper is the REST layer's, not the store's. */
    @Test
    void scheduleRejectsAPayloadWithAnUnknownPropertyAs422() throws Exception {
        JobKey key = JobKey.of("welcome-email");
        when(mohs.findJob(key)).thenReturn(Optional.of(snapshot("welcome-email", false)));
        when(mohs.jobRef(key)).thenReturn(Optional.of(JobRef.of(key.value(), CountPayload.class)));

        mockMvc.perform(post(ApiPaths.V1 + "/jobs/welcome-email/schedule")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"payload\":{\"count\":1,\"cuont\":2}}"))
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.field").value("payload"))
                .andExpect(jsonPath("$.detail").value("payload does not match the job's declared input schema"))
                .andExpect(result -> assertThat(result.getResponse().getContentAsString())
                        .doesNotContain("CountPayload", "cuont", "tools.jackson"));
    }

    /** The convertValue branch: in Jackson 3 a databind failure is NOT an IllegalArgumentException — without the right catch this was a 500, not the contract's 422. */
    @Test
    void scheduleRejectsAPayloadIncompatibleWithTheDeclaredTypeAs422() throws Exception {
        JobKey key = JobKey.of("welcome-email");
        when(mohs.findJob(key)).thenReturn(Optional.of(snapshot("welcome-email", false)));
        when(mohs.jobRef(key)).thenReturn(Optional.of(JobRef.of(key.value(), CountPayload.class)));

        mockMvc.perform(post(ApiPaths.V1 + "/jobs/welcome-email/schedule")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"payload\":{\"count\":\"not-a-number\"}}"))
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.field").value("payload"))
                .andExpect(result -> {
                    String body = result.getResponse().getContentAsString();
                    assertThat(body).doesNotContain("CountPayload", "not-a-number", "tools.jackson");
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
        Execution first = new Execution(ExecutionId.of("01991337-0000-7000-8000-00000000001e"), key, ExecutionState.ENQUEUED, NOW, null, Optional.empty(), "tester");
        Execution second = new Execution(ExecutionId.of("01991337-0000-7000-8000-00000000001c"), key, ExecutionState.ENQUEUED, NOW.minusSeconds(1), null, Optional.empty(), "tester");
        when(mohs.executions(any())).thenReturn(List.of(first, second));

        mockMvc.perform(get(ApiPaths.V1 + "/jobs/welcome-email/executions").param("size", "1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.items.length()").value(1))
                .andExpect(jsonPath("$.items[0].executionId").value("01991337-0000-7000-8000-00000000001e"))
                .andExpect(jsonPath("$.nextCursor").value("01991337-0000-7000-8000-00000000001e"));
    }

    /** size=0 and size=-1 saturate at 1 (CursorPage.clampSize) — previously 0 threw IndexOutOfBounds and a negative threw IAE, both 500s. */
    @Test
    void executionsSaturatesANonPositiveSizeInsteadOfFailing() throws Exception {
        JobKey key = JobKey.of("welcome-email");
        when(mohs.findJob(key)).thenReturn(Optional.of(snapshot("welcome-email", false)));
        Execution first = new Execution(ExecutionId.of("01991337-0000-7000-8000-00000000001e"), key, ExecutionState.ENQUEUED, NOW, null, Optional.empty(), "tester");
        Execution second = new Execution(ExecutionId.of("01991337-0000-7000-8000-00000000001c"), key, ExecutionState.ENQUEUED, NOW.minusSeconds(1), null, Optional.empty(), "tester");
        when(mohs.executions(any())).thenReturn(List.of(first, second));

        mockMvc.perform(get(ApiPaths.V1 + "/jobs/welcome-email/executions").param("size", "0"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.items.length()").value(1));

        mockMvc.perform(get(ApiPaths.V1 + "/jobs/welcome-email/executions").param("size", "-1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.items.length()").value(1));
    }
}
