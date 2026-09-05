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
package io.mohs.rest.error;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ProblemDetail;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.mock.http.MockHttpInputMessage;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.web.context.request.ServletWebRequest;

import tools.jackson.databind.exc.ValueInstantiationException;
import tools.jackson.databind.json.JsonMapper;

import io.mohs.core.execution.ExecutionId;
import io.mohs.core.job.JobKey;
import io.mohs.rest.ratelimit.RateLimitPatchRequest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RestExceptionHandlerTest {

    private final RestExceptionHandler handler = new RestExceptionHandler();

    @Test
    void jobNotFoundBecomes404WithNearbySuggestions() {
        JobNotFoundException ex = new JobNotFoundException(
                JobKey.of("wlecome-email"), List.of(JobKey.of("welcome-email")));

        ProblemDetail problem = handler.handleJobNotFound(ex);

        assertThat(problem.getStatus()).isEqualTo(HttpStatus.NOT_FOUND.value());
        assertThat(problem.getProperties()).containsKey("nearbyJobKeys");
        assertThat(problem.getProperties().get("nearbyJobKeys")).isEqualTo(List.of("welcome-email"));
    }

    @Test
    void jobNotFoundWithNoSuggestionsOmitsTheProperty() {
        JobNotFoundException ex = new JobNotFoundException(JobKey.of("ghost"), List.of());

        ProblemDetail problem = handler.handleJobNotFound(ex);

        assertThat(problem.getProperties()).isNull();
    }

    @Test
    void executionNotFoundBecomes404() {
        ExecutionNotFoundException ex = new ExecutionNotFoundException(ExecutionId.of("exec-1"));

        ProblemDetail problem = handler.handleExecutionNotFound(ex);

        assertThat(problem.getStatus()).isEqualTo(HttpStatus.NOT_FOUND.value());
        assertThat(problem.getDetail()).isEqualTo("The requested execution was not found");
    }

    @Test
    void missingResourcesDoNotReflectUnboundedRequestIdentifiers() {
        String supplied = "private-request-value".repeat(1_000);
        assertThat(handler.handleRateLimitNotFound(new RateLimitNotFoundException(supplied)).getDetail())
                .doesNotContain(supplied).hasSizeLessThan(300);
        assertThat(handler.handleBatchNotFound(new BatchNotFoundException(supplied)).getDetail())
                .doesNotContain(supplied).hasSizeLessThan(300);
        assertThat(handler.handleExecutionNotFound(new ExecutionNotFoundException(ExecutionId.of(supplied))).getDetail())
                .doesNotContain(supplied).hasSizeLessThan(300);
    }

    @Test
    void aConversionCauseIsNotMistakenForAPublicDomainValidationMessage() {
        var request = new org.springframework.web.context.request.ServletWebRequest(
                new org.springframework.mock.web.MockHttpServletRequest());
        var failure = new org.springframework.http.converter.HttpMessageNotReadableException(
                "conversion failed", new IllegalArgumentException("host.SecretPayload: private-request-value"),
                new org.springframework.mock.http.MockHttpInputMessage(new byte[0]));

        var response = handler.handleHttpMessageNotReadable(failure, new org.springframework.http.HttpHeaders(),
                HttpStatus.BAD_REQUEST, request);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(((ProblemDetail) response.getBody()).getDetail())
                .doesNotContain("SecretPayload", "private-request-value");
    }

    @Test
    void constructorValidationAtTheLastAllowedDepthStillProduces422() {
        ProblemDetail problem = unreadableProblem(validationAtDepth(64));

        assertThat(problem.getStatus()).isEqualTo(422);
        assertThat(problem.getDetail()).isEqualTo("max must be at least 1");
    }

    @Test
    void constructorValidationBeyondTheDepthLimitUsesTheGenericResponse() {
        ProblemDetail problem = unreadableProblem(validationAtDepth(65));

        assertThat(problem.getStatus()).isEqualTo(400);
        assertThat(problem.getDetail()).isEqualTo("Failed to read request");
    }

    @Test
    void aCyclicCauseChainTerminatesWithTheGenericResponse() {
        Throwable cycle = mock(Throwable.class);
        AtomicInteger reads = new AtomicInteger();
        when(cycle.getCause()).thenAnswer(_ -> {
            // Fail a missing bound deterministically instead of leaving a busy thread behind a timeout.
            assertThat(reads.incrementAndGet()).as("cause traversal is bounded").isLessThanOrEqualTo(64);
            return cycle;
        });

        ProblemDetail problem = unreadableProblem(unreadable(cycle));

        assertThat(problem.getStatus()).isEqualTo(400);
        assertThat(problem.getDetail()).isEqualTo("Failed to read request");
    }

    /** The HTTP wrapper is depth 1; Jackson's constructor exception is exactly at the requested depth. */
    private static HttpMessageNotReadableException validationAtDepth(int depth) {
        Throwable cause = ValueInstantiationException.from(null, "internal constructor diagnostics",
                JsonMapper.builder().build().getTypeFactory().constructType(RateLimitPatchRequest.class),
                new IllegalArgumentException("max must be at least 1"));
        for (int i = 2; i < depth; i++) {
            cause = new IllegalStateException("wrapper", cause);
        }
        return unreadable(cause);
    }

    private static HttpMessageNotReadableException unreadable(Throwable cause) {
        return new HttpMessageNotReadableException("conversion failed", cause, new MockHttpInputMessage(new byte[0]));
    }

    private ProblemDetail unreadableProblem(HttpMessageNotReadableException failure) {
        var response = handler.handleHttpMessageNotReadable(failure, new HttpHeaders(), HttpStatus.BAD_REQUEST,
                new ServletWebRequest(new MockHttpServletRequest()));
        return (ProblemDetail) response.getBody();
    }

    @Test
    void payloadValidationBecomes422AndPointsAtTheField() {
        PayloadValidationException ex = new PayloadValidationException("age", "must be a non-negative integer");

        ProblemDetail problem = handler.handlePayloadValidation(ex);

        assertThat(problem.getStatus()).isEqualTo(HttpStatus.UNPROCESSABLE_CONTENT.value());
        assertThat(problem.getProperties()).containsEntry("field", "age");
    }

    @Test
    void invalidActorBecomes400WithTheTeachingDetail() {
        InvalidActorException ex = new InvalidActorException("X-Mohs-Actor must be at most 255 characters, got 300");

        ProblemDetail problem = handler.handleInvalidActor(ex);

        assertThat(problem.getStatus()).isEqualTo(HttpStatus.BAD_REQUEST.value());
        assertThat(problem.getDetail()).contains("255");
    }

    /** MohsRestAutoConfiguration's rationale: a v1 contract without an implementation is an honest 501, never a 500 "unexpected". */
    @Test
    void unsupportedOperationBecomes501() {
        ProblemDetail problem = handler.handleNotImplemented(new UnsupportedOperationException("not implemented yet"));

        assertThat(problem.getStatus()).isEqualTo(HttpStatus.NOT_IMPLEMENTED.value());
        assertThat(problem.getTitle()).isEqualTo("Not implemented");
    }

    /** The message of a contention failure is the driver's, and a driver names the statement — the 503 must not carry it to the client. */
    @Test
    void rowContentionBecomes503WithoutLeakingTheQuery() {
        QueryTimeoutException ex = new QueryTimeoutException("lock wait on SELECT ... FROM mohs_rate_limits FOR UPDATE");

        ProblemDetail problem = handler.handleContention(ex);

        assertThat(problem.getStatus()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE.value());
        assertThat(problem.getTitle()).isEqualTo("Resource busy");
        assertThat(problem.getDetail()).doesNotContain("mohs_rate_limits");
    }

    @Test
    void unexpectedExceptionBecomes500WithoutLeakingItsMessage() {
        RuntimeException ex = new RuntimeException("connection string: postgres://user:secret@host/db");

        ProblemDetail problem = handler.handleUnexpected(ex);

        assertThat(problem.getStatus()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR.value());
        assertThat(problem.getDetail()).doesNotContain("secret");
    }
}
