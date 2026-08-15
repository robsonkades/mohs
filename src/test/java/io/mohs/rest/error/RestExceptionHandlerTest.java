package io.mohs.rest.error;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;

import io.mohs.core.execution.ExecutionId;
import io.mohs.core.job.JobKey;

import static org.assertj.core.api.Assertions.assertThat;

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
        assertThat(problem.getDetail()).contains("exec-1");
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

    /** Racional de MohsRestAutoConfiguration: contrato v1 sem implementação é 501 honesto, nunca 500 "unexpected". */
    @Test
    void unsupportedOperationBecomes501() {
        ProblemDetail problem = handler.handleNotImplemented(new UnsupportedOperationException("M3: ainda não implementado"));

        assertThat(problem.getStatus()).isEqualTo(HttpStatus.NOT_IMPLEMENTED.value());
        assertThat(problem.getTitle()).isEqualTo("Not implemented");
    }

    @Test
    void unexpectedExceptionBecomes500WithoutLeakingItsMessage() {
        RuntimeException ex = new RuntimeException("connection string: postgres://user:secret@host/db");

        ProblemDetail problem = handler.handleUnexpected(ex);

        assertThat(problem.getStatus()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR.value());
        assertThat(problem.getDetail()).doesNotContain("secret");
    }
}
