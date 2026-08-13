package io.mohs.rest.error;

import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

import io.mohs.core.job.JobKey;

/**
 * Traduz as exceções de domínio REST para {@link ProblemDetail} (RFC 7807,
 * {@code application/problem+json}) — "erros com {@code detail} que ensina
 * a corrigir" (ver {@code docs/adr/0010-rest-api-v1.md}). Estende
 * {@link ResponseEntityExceptionHandler} pra herdar de graça a tradução de
 * erros de framework (JSON malformado, parâmetro ausente) via
 * {@code spring.mvc.problemdetails} — só precisa tratar as 3 exceções de
 * domínio abaixo. Sem estado, sem dependência de construtor.
 *
 * <p>{@code type} fica no default RFC 7807 ({@code about:blank}) — o
 * domínio {@code mohs.io} ainda não tem registro confirmado (ver
 * {@code docs/MOHS-DOCUMENTO-MESTRE.md} §7), então nenhuma URI própria é
 * inventada aqui ainda.
 */
@RestControllerAdvice
public class RestExceptionHandler extends ResponseEntityExceptionHandler {

    @ExceptionHandler(JobNotFoundException.class)
    public ProblemDetail handleJobNotFound(JobNotFoundException ex) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.NOT_FOUND, ex.getMessage());
        problem.setTitle("Job not found");
        if (!ex.nearbyJobKeys().isEmpty()) {
            problem.setProperty("nearbyJobKeys", ex.nearbyJobKeys().stream().map(JobKey::value).toList());
        }
        return problem;
    }

    @ExceptionHandler(ExecutionNotFoundException.class)
    public ProblemDetail handleExecutionNotFound(ExecutionNotFoundException ex) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.NOT_FOUND, ex.getMessage());
        problem.setTitle("Execution not found");
        return problem;
    }

    @ExceptionHandler(PayloadValidationException.class)
    public ProblemDetail handlePayloadValidation(PayloadValidationException ex) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.UNPROCESSABLE_ENTITY, ex.getMessage());
        problem.setTitle("Payload validation failed");
        problem.setProperty("field", ex.field());
        return problem;
    }
}
