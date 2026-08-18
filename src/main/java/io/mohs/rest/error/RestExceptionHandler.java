package io.mohs.rest.error;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

import io.mohs.core.job.JobKey;

/**
 * Traduz as exceções de domínio REST para {@link ProblemDetail} (RFC 7807,
 * {@code application/problem+json}) — "erros com {@code detail} que ensina
 * a corrigir" (ver {@code docs/adr/0010-rest-api-v1.md}). Estende
 * {@link ResponseEntityExceptionHandler} pra herdar de graça a tradução de
 * erros de framework (JSON malformado, parâmetro ausente) via
 * {@code spring.mvc.problemdetails} — só precisa tratar as exceções de
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
     * {@code ResponseEntityExceptionHandler.handleHttpMessageNotReadable}
     * base substitui a mensagem original por um {@code detail} fixo
     * ("Failed to read request") — perde a validação bem redigida que os
     * records de request (ex.: {@link io.mohs.rest.ratelimit.RateLimitPatchRequest})
     * já fazem no próprio compact constructor, disparada durante a
     * desserialização do Jackson. Quando a causa raiz é uma {@link
     * IllegalArgumentException} (validação de domínio, não JSON malformado),
     * devolve 422 com a mensagem original — mesmo formato de {@link
     * #handlePayloadValidation}, sem {@code field} estruturado porque essa
     * exceção não carrega um (a mensagem já nomeia o campo por convenção,
     * ex.: "max must be at least 1"). JSON genuinamente malformado (sem
     * {@link IllegalArgumentException} na cadeia de causa) continua caindo
     * no comportamento default do Spring.
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
     * Operação do contrato v1 ainda sem implementação (ex.:
     * {@code OverviewController}, {@code BatchesController}) — 501 honesto em vez de
     * o {@code UnsupportedOperationException} virar 500 "unexpected error"
     * com stack trace no log: é exatamente o racional que
     * {@code MohsRestAutoConfiguration} usa pra nem registrar os
     * controllers M2 sem implementação.
     */
    @ExceptionHandler(UnsupportedOperationException.class)
    public ProblemDetail handleNotImplemented(UnsupportedOperationException ex) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.NOT_IMPLEMENTED,
                "This operation is part of the v1 contract but is not implemented yet");
        problem.setTitle("Not implemented");
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
