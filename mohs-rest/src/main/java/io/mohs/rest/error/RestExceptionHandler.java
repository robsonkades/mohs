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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.PessimisticLockingFailureException;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

import io.mohs.core.job.JobKey;

/**
 * Translates the REST domain exceptions into {@link ProblemDetail} (RFC 7807,
 * {@code application/problem+json}) — "errors whose {@code detail} teaches you how to fix them".
 *
 * <p>It extends {@link ResponseEntityExceptionHandler} to inherit the translation of framework
 * errors (malformed JSON, a missing parameter) for free through
 * {@code spring.mvc.problemdetails}, leaving only the domain exceptions below to handle. Stateless,
 * with no constructor dependency.
 *
 * <p>{@code type} stays at RFC 7807's default ({@code about:blank}) — the {@code mohs.io} domain has
 * no confirmed registry yet, so no URI of our own is invented here.
 */
/*
 * Scoped to the package of Mohs's controllers, and NEVER global: a @RestControllerAdvice without
 * basePackages applies to every controller in the context, so turning on mohs.api.enabled would
 * start deciding the error handling of the HOST APPLICATION — one of its @ResponseStatus(NOT_FOUND)
 * would become a 500 because of the @ExceptionHandler(Exception.class) here, without a line of the
 * app changing.
 *
 * HIGHEST_PRECEDENCE because, on Mohs's endpoints, the house advice has to beat a generic advice
 * from the host; without it, the tie between two ResponseEntityExceptionHandlers at
 * LOWEST_PRECEDENCE falls to bean registration order. The same care MohsUiAutoConfiguration already
 * takes with static resources, applied to the exception axis.
 */
@RestControllerAdvice(basePackages = "io.mohs.rest")
@Order(Ordered.HIGHEST_PRECEDENCE)
public class RestExceptionHandler extends ResponseEntityExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(RestExceptionHandler.class);

    @ExceptionHandler(RateLimitNotFoundException.class)
    public ProblemDetail handleRateLimitNotFound(RateLimitNotFoundException ex) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.NOT_FOUND,
                ex.getMessage() + " — declare it with mohs.rate-limits." + ex.rateLimitName()
                        + ".max/.window (or a @Bean RateLimit) and restart; PATCH only adjusts what boot declared");
        problem.setTitle("Rate limit not found");
        return problem;
    }

    @ExceptionHandler(JobNotFoundException.class)
    public ProblemDetail handleJobNotFound(JobNotFoundException ex) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.NOT_FOUND, ex.getMessage());
        problem.setTitle("Job not found");
        if (!ex.nearbyJobKeys().isEmpty()) {
            problem.setProperty("nearbyJobKeys", ex.nearbyJobKeys().stream().map(JobKey::value).toList());
        }
        return problem;
    }

    @ExceptionHandler(BatchNotFoundException.class)
    public ProblemDetail handleBatchNotFound(BatchNotFoundException ex) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.NOT_FOUND, ex.getMessage());
        problem.setTitle("Batch not found");
        return problem;
    }

    @ExceptionHandler(ExecutionNotFoundException.class)
    public ProblemDetail handleExecutionNotFound(ExecutionNotFoundException ex) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.NOT_FOUND, ex.getMessage());
        problem.setTitle("Execution not found");
        return problem;
    }

    @ExceptionHandler(ExecutionNotRetryableException.class)
    public ProblemDetail handleExecutionNotRetryable(ExecutionNotRetryableException ex) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.CONFLICT, ex.getMessage());
        problem.setTitle("Execution not retryable");
        return problem;
    }

    @ExceptionHandler(InvalidActorException.class)
    public ProblemDetail handleInvalidActor(InvalidActorException ex) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, ex.getMessage());
        problem.setTitle("Invalid actor");
        return problem;
    }

    @ExceptionHandler(PayloadValidationException.class)
    public ProblemDetail handlePayloadValidation(PayloadValidationException ex) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.UNPROCESSABLE_CONTENT, ex.getMessage());
        problem.setTitle("Request validation failed");
        problem.setProperty("field", ex.field());
        return problem;
    }

    /**
     * The base {@code ResponseEntityExceptionHandler.handleHttpMessageNotReadable} replaces the
     * original message with a fixed {@code detail} ("Failed to read request"), losing the
     * well-written validation the request records (e.g.
     * {@link io.mohs.rest.ratelimit.RateLimitPatchRequest}) already perform in their compact
     * constructors, thrown during Jackson's deserialisation.
     *
     * <p>When the root cause is an {@link IllegalArgumentException} (domain validation, not
     * malformed JSON), it returns a 422 with the original message — the same shape as
     * {@link #handlePayloadValidation}, without a structured {@code field} because that exception
     * does not carry one (the message already names the field by convention, e.g. "max must be at
     * least 1"). Genuinely malformed JSON, with no {@link IllegalArgumentException} in the cause
     * chain, keeps falling through to Spring's default behaviour.
     */
    @Override
    protected ResponseEntity<Object> handleHttpMessageNotReadable(
            HttpMessageNotReadableException ex, HttpHeaders headers, HttpStatusCode status, WebRequest request) {
        Throwable cause = ex;
        while (cause != null && !(cause instanceof IllegalArgumentException)) {
            cause = cause.getCause();
        }
        if (cause == null) {
            return super.handleHttpMessageNotReadable(ex, headers, status, request);
        }
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.UNPROCESSABLE_CONTENT, cause.getMessage());
        problem.setTitle("Request validation failed");
        return new ResponseEntity<>(problem, HttpStatus.UNPROCESSABLE_CONTENT);
    }

    /**
     * A row contended by the engine's hot path — today only the {@code mohs_rate_limits} bucket,
     * whose lock has a time ceiling ({@code JdbcRateLimitStore#BUCKET_LOCK_TIMEOUT}).
     *
     * <p>A 503 meaning "nothing changed", not a generic 500: under contention the {@code PATCH} is
     * precisely the emergency lever the operator is pulling, and "unexpected error" would leave them
     * unsure whether it applied — retrying on top of an already saturated row. Transient by
     * definition ({@code TransientDataAccessException}): the same request repeated later tends to
     * succeed.
     */
    @ExceptionHandler({QueryTimeoutException.class, PessimisticLockingFailureException.class})
    public ProblemDetail handleContention(TransientDataAccessException ex) {
        log.warn("a database row is under contention — the requested change did not apply", ex);
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.SERVICE_UNAVAILABLE,
                "A database row is under contention and nothing changed — retry in a few seconds");
        problem.setTitle("Resource busy");
        return problem;
    }

    /**
     * Every v1 route is implemented today, so this handler is a net rather than a route: an honest
     * 501 for an operation that declares itself unimplemented, instead of letting the
     * {@code UnsupportedOperationException} become a 500 "unexpected error" with a stack trace in
     * the log. It stays because the alternative on the day a contract route lands ahead of its
     * implementation is the worst of the two answers.
     */
    @ExceptionHandler(UnsupportedOperationException.class)
    public ProblemDetail handleNotImplemented(UnsupportedOperationException ex) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.NOT_IMPLEMENTED,
                "This operation is part of the v1 contract but is not implemented yet");
        problem.setTitle("Not implemented");
        return problem;
    }

    /**
     * A safety net for any unforeseen exception (an infrastructure failure, a bug) — without it,
     * that would fall through to Boot's default {@code /error}, whose body is structurally different
     * from RFC 7807. The real cause goes only to the server's log — never {@code ex.getMessage()} in
     * the body, so as not to leak internal detail to an untrusted caller.
     */
    @ExceptionHandler(Exception.class)
    public ProblemDetail handleUnexpected(Exception ex) {
        log.error("unhandled exception reaching the REST layer", ex);
        return ProblemDetail.forStatusAndDetail(HttpStatus.INTERNAL_SERVER_ERROR, "An unexpected error occurred");
    }
}
