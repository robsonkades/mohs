package io.mohs.rest.error;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

    private static final Logger log = LoggerFactory.getLogger(RestExceptionHandler.class);

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
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.UNPROCESSABLE_CONTENT, ex.getMessage());
        problem.setTitle("Payload validation failed");
        problem.setProperty("field", ex.field());
        return problem;
    }

    /**
     * Rede de segurança pra qualquer exceção não prevista (falha de
     * infraestrutura, bug) — sem isso, ela cai no {@code /error} padrão do
     * Boot, com corpo estruturalmente diferente de RFC 7807 (REST-2). A
     * causa real vai só pro log do servidor — nunca {@code ex.getMessage()}
     * no corpo, pra não vazar detalhe interno pra um chamador não confiável.
     */
    @ExceptionHandler(Exception.class)
    public ProblemDetail handleUnexpected(Exception ex) {
        log.error("unhandled exception reaching the REST layer", ex);
        return ProblemDetail.forStatusAndDetail(HttpStatus.INTERNAL_SERVER_ERROR, "An unexpected error occurred");
    }
}
