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
import io.mohs.rest.ActorResolver;
import io.mohs.rest.ApiPaths;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Comportamento real de {@link JobsController} sobre uma {@link Mohs}
 * mockada — {@code mohs.enabled=false} desliga {@code MohsAutoConfiguration}
 * (que exigiria {@code DataSource}) nesta fatia de teste. {@code
 * JobsController} entra via {@code @Bean} explícito, não via {@code
 * @WebMvcTest(controllers = ...)}: {@link io.mohs.MohsApplication} exclui
 * {@code io.mohs.rest..*} do component-scan (correção necessária pra
 * {@code MohsRestAutoConfiguration} não colidir com o próprio scan em
 * runtime), e o slice de teste herda esse mesmo scan da configuração
 * {@code @SpringBootConfiguration} mais próxima — a exclusão bloquearia
 * a inclusão explícita de {@code controllers = JobsController.class}
 * também.
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

    /** Invocação completa: delay (computado no servidor) e priority chegam ao ScheduleCommand — os três terminais do core na borda REST. */
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

    /** at × delay são exclusivos — a validação do record dispara na desserialização e vira 422 que ensina. */
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

    /** O ramo do convertValue: no Jackson 3 a falha de databind NÃO é IllegalArgumentException — sem o catch certo isto era 500, não o 422 do contrato. */
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

    /** size=0/-1 saturam em 1 (CursorPage.clampSize) — antes, 0 estourava IndexOutOfBounds e negativo estourava IAE, ambos 500. */
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
