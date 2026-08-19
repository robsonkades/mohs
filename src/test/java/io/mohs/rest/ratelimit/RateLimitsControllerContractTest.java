package io.mohs.rest.ratelimit;

import java.time.Duration;
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

import io.mohs.core.Mohs;
import io.mohs.core.RateLimitSnapshot;
import io.mohs.core.resource.RateLimit;
import io.mohs.rest.ActorResolver;
import io.mohs.rest.ApiPaths;
import io.mohs.rest.HeaderActorResolver;
import io.mohs.rest.RuntimePatchResponse;
import io.mohs.rest.error.RestExceptionHandler;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.patch;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Ver Javadoc de {@link io.mohs.rest.job.JobsControllerContractTest} sobre
 * o {@code @TestConfiguration}/{@code mohs.enabled=false} — mesmo motivo
 * aqui: {@link io.mohs.MohsApplication} exclui {@code io.mohs.rest..*} do
 * component-scan. {@link RestExceptionHandler} entra também: os testes de
 * 404 e 422 dependem dele.
 */
@WebMvcTest(properties = "mohs.enabled=false")
class RateLimitsControllerContractTest {

    @TestConfiguration
    static class ControllerConfig {

        @Bean
        RateLimitsController rateLimitsController(Mohs mohs, ActorResolver actorResolver) {
            return new RateLimitsController(mohs, actorResolver);
        }

        @Bean
        ActorResolver actorResolver() {
            return new HeaderActorResolver();
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

    @Test
    void listExposesTheSpecAndTheCurrentBucketBalance() throws Exception {
        when(mohs.rateLimits()).thenReturn(List.of(
                new RateLimitSnapshot(new RateLimit("smtp", 100, Duration.ofMinutes(1)), 42)));

        mockMvc.perform(get(ApiPaths.V1 + "/rate-limits"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].name").value("smtp"))
                .andExpect(jsonPath("$[0].max").value(100))
                .andExpect(jsonPath("$[0].window").value("PT1M"))
                .andExpect(jsonPath("$[0].available").value(42));
    }

    @Test
    void patchAdjustsTheLimitAndCarriesTheBootReversionNotice() throws Exception {
        when(mohs.adjustRateLimit(eq("smtp"), eq(250), any()))
                .thenReturn(Optional.of(new RateLimitSnapshot(new RateLimit("smtp", 250, Duration.ofSeconds(30)), 250)));

        mockMvc.perform(patch(ApiPaths.V1 + "/rate-limits/smtp")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"max\":250,\"window\":\"PT30S\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.resource.max").value(250))
                .andExpect(jsonPath("$.notice").value(RuntimePatchResponse.BOOT_REVERSION_NOTICE));
    }

    /** Declarar limite é ato de boot (ADR-0042): PATCH em nome inexistente é 404 que ensina onde declarar, nunca criação implícita. */
    @Test
    void patchOnAnUnknownRateLimitIs404() throws Exception {
        when(mohs.adjustRateLimit(eq("ghost"), eq(10), any())).thenReturn(Optional.empty());

        mockMvc.perform(patch(ApiPaths.V1 + "/rate-limits/ghost")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"max\":10,\"window\":\"PT1M\"}"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.title").value("Rate limit not found"))
                .andExpect(jsonPath("$.detail").value(org.hamcrest.Matchers.containsString("mohs.rate-limits.ghost.max")));
    }

    /** Actor é inegociável em mutação (ADR-0010) — o header declarado chega ao resolver antes da escrita. */
    @Test
    void patchResolvesTheActorBeforeMutating() throws Exception {
        when(mohs.adjustRateLimit(eq("smtp"), eq(10), any()))
                .thenReturn(Optional.of(new RateLimitSnapshot(new RateLimit("smtp", 10, Duration.ofMinutes(1)), 10)));

        mockMvc.perform(patch(ApiPaths.V1 + "/rate-limits/smtp")
                        .header("X-Mohs-Actor", "ana.ops")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"max\":10,\"window\":\"PT1M\"}"))
                .andExpect(status().isOk());

        verify(mohs).adjustRateLimit("smtp", 10, Duration.ofMinutes(1));
    }

    /**
     * REST-3 (docs/codereview.md): a validação de {@link RateLimitPatchRequest}
     * roda no compact constructor, disparada durante a desserialização do
     * Jackson — sem {@code io.mohs.rest.error.RestExceptionHandler#handleHttpMessageNotReadable}
     * interceptando, o Spring substituiria a mensagem original por "Failed
     * to read request" e devolveria 400 genérico. Prova que a mensagem real
     * chega ao cliente, com o status 422 que o design REST promete pra
     * payload inválido.
     */
    @Test
    void patchWithInvalidBodySurfacesTheValidationMessageAs422() throws Exception {
        mockMvc.perform(patch(ApiPaths.V1 + "/rate-limits/smtp")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"max\":0,\"window\":\"PT1M\"}"))
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.detail").value("max must be at least 1"));
    }
}
