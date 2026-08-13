package io.mohs.rest.execution;

import jakarta.servlet.http.HttpServletRequest;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;

import io.mohs.core.execution.ExecutionState;
import io.mohs.rest.AcceptedExecutionResponse;
import io.mohs.rest.CursorPage;

/**
 * Área de recurso "executions" (ver {@code docs/REST-API-DESIGN.md}). Busca
 * global (cursor), detalhe, cancelamento cooperativo e retry manual.
 */
@RestController
@RequestMapping("/api/mohs/v1/executions")
public class ExecutionsController {

    @GetMapping
    public CursorPage<ExecutionResponse> search(
            @RequestParam(required = false) ExecutionState status,
            @RequestParam(required = false) String jobKey,
            @RequestParam(required = false) Instant from,
            @RequestParam(required = false) Instant to,
            @RequestParam(required = false) String cursor) {
        throw new UnsupportedOperationException("M3: ainda não implementado");
    }

    @GetMapping("/{id}")
    public ExecutionResponse get(@PathVariable String id) {
        throw new UnsupportedOperationException("M3: ainda não implementado");
    }

    /** Cancelamento é cooperativo, não imediato — 202 com o estado atual, não necessariamente terminal. */
    @PostMapping("/{id}/cancel")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public ExecutionResponse cancel(@PathVariable String id, HttpServletRequest request) {
        throw new UnsupportedOperationException("M3: ainda não implementado");
    }

    /** Retry manual bypassa política de retry exaurida — mesmo contrato de aceite de {@code schedule}. */
    @PostMapping("/{id}/retry")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public ResponseEntity<AcceptedExecutionResponse> retry(@PathVariable String id, HttpServletRequest request) {
        throw new UnsupportedOperationException("M3: ainda não implementado");
    }
}
