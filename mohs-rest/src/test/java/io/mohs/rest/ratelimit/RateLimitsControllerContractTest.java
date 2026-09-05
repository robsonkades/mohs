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
 * See {@link io.mohs.rest.job.JobsControllerContractTest}'s Javadoc about the
 * {@code @TestConfiguration}/{@code mohs.enabled=false} pair — the same reason applies here:
 * {@link io.mohs.rest.RestSliceConfiguration} does not component-scan {@code io.mohs.rest}.
 * {@link RestExceptionHandler} comes along too: the 404 and 422 tests depend on it.
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

    /** Declaring a limit is an act of boot: a PATCH on a nonexistent name is a 404 that teaches where to declare it, never implicit creation. */
    @Test
    void patchOnAnUnknownRateLimitIs404() throws Exception {
        when(mohs.adjustRateLimit(eq("ghost"), eq(10), any())).thenReturn(Optional.empty());

        mockMvc.perform(patch(ApiPaths.V1 + "/rate-limits/ghost")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"max\":10,\"window\":\"PT1M\"}"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.title").value("Rate limit not found"))
                .andExpect(jsonPath("$.detail").value(org.hamcrest.Matchers.containsString("mohs.rate-limits.<name>.max")));
    }

    /** An actor is non-negotiable on a mutation — the declared header reaches the resolver before the write. */
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
     * {@link RateLimitPatchRequest}'s validation runs in the compact constructor, fired during
     * Jackson's deserialisation — without
     * {@code io.mohs.rest.error.RestExceptionHandler#handleHttpMessageNotReadable} intercepting,
     * Spring would replace the original message with "Failed to read request" and return a generic
     * 400. This proves the real message reaches the client, with the 422 the REST design promises for
     * an invalid payload.
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
