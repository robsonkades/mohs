/**
 * Tradução de exceções REST para RFC 7807 ({@link org.springframework.http.ProblemDetail}):
 * {@link io.mohs.rest.error.JobNotFoundException},
 * {@link io.mohs.rest.error.ExecutionNotFoundException},
 * {@link io.mohs.rest.error.PayloadValidationException} e o
 * {@code @RestControllerAdvice} que as traduz,
 * {@link io.mohs.rest.error.RestExceptionHandler}. Depende de
 * {@code io.mohs.core.job} ({@code JobKey}) e {@code io.mohs.core.execution}
 * ({@code ExecutionId}).
 */
@NullMarked
package io.mohs.rest.error;

import org.jspecify.annotations.NullMarked;
