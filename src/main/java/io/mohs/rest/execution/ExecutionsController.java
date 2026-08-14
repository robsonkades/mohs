package io.mohs.rest.execution;

import jakarta.servlet.http.HttpServletRequest;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;

import io.mohs.core.execution.ExecutionState;
import io.mohs.rest.AcceptedExecutionResponse;
import io.mohs.rest.ApiPaths;
import io.mohs.rest.CursorPage;

/**
 * Área de recurso "executions" (ver {@code docs/REST-API-DESIGN.md}). Busca
 * global (cursor), detalhe, cancelamento cooperativo e retry manual.
 */
@RestController
@RequestMapping(ApiPaths.V1 + "/executions")
public class ExecutionsController {

    /** {@code size} — ver {@link CursorPage#DEFAULT_PAGE_SIZE}/{@link CursorPage#MAX_PAGE_SIZE}. */
    @GetMapping
    public CursorPage<ExecutionResponse> search(
            @RequestParam(required = false) ExecutionState status,
            @RequestParam(required = false) String jobKey,
            @RequestParam(required = false) Instant from,
            @RequestParam(required = false) Instant to,
            @RequestParam(required = false) String cursor,
            @RequestParam(required = false) Integer size) {
        throw new UnsupportedOperationException("M3: ainda não implementado");
    }

    @GetMapping("/{id}")
    public ExecutionResponse get(@PathVariable String id) {
        throw new UnsupportedOperationException("M3: ainda não implementado");
    }

    /**
     * Cancelamento é cooperativo, não imediato — 202 com o estado atual, não
     * necessariamente terminal. {@code ResponseEntity} (não {@code
     * ExecutionResponse} puro) pelo mesmo motivo de {@link #retry}: é onde o
     * header {@code Location: /executions/{id}} do princípio 1 do design REST
     * é anexado — {@code @ResponseStatus} não teria efeito aqui (REST-1).
     */
    @PostMapping("/{id}/cancel")
    public ResponseEntity<ExecutionResponse> cancel(@PathVariable String id, HttpServletRequest request) {
        throw new UnsupportedOperationException("M3: ainda não implementado");
    }

    /**
     * Retry manual bypassa política de retry exaurida — mesmo contrato de
     * aceite de {@code schedule}. Status 202 vem de
     * {@link ResponseEntity#getStatusCode()} no corpo do método —
     * {@code @ResponseStatus} não tem efeito sobre {@code ResponseEntity} (REST-1).
     */
    @PostMapping("/{id}/retry")
    public ResponseEntity<AcceptedExecutionResponse> retry(
            @PathVariable String id, @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey, HttpServletRequest request) {
        throw new UnsupportedOperationException("M3: ainda não implementado");
    }
}
