package io.mohs.rest.execution;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

import jakarta.servlet.http.HttpServletRequest;

import org.jspecify.annotations.Nullable;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import io.mohs.core.ExecutionQuery;
import io.mohs.core.Mohs;
import io.mohs.core.execution.Execution;
import io.mohs.core.execution.ExecutionId;
import io.mohs.core.execution.ExecutionState;
import io.mohs.core.job.JobKey;
import io.mohs.rest.AcceptedExecutionResponse;
import io.mohs.rest.ApiPaths;
import io.mohs.rest.CursorPage;
import io.mohs.rest.error.ExecutionNotFoundException;

/**
 * Área de recurso "executions" (ver {@code docs/REST-API-DESIGN.md}). Busca
 * global (cursor), detalhe, cancelamento cooperativo e retry manual.
 */
@RestController
@RequestMapping("${mohs.api.base-path:" + ApiPaths.V1 + "}/executions")
public class ExecutionsController {

    private final Mohs mohs;

    public ExecutionsController(Mohs mohs) {
        this.mohs = Objects.requireNonNull(mohs, "mohs");
    }

    /** {@code size} — ver {@link CursorPage#DEFAULT_PAGE_SIZE}/{@link CursorPage#MAX_PAGE_SIZE}. */
    @GetMapping
    public CursorPage<ExecutionResponse> search(
            @RequestParam(required = false) @Nullable ExecutionState status,
            @RequestParam(required = false) @Nullable String jobKey,
            @RequestParam(required = false) @Nullable Instant from,
            @RequestParam(required = false) @Nullable Instant to,
            @RequestParam(required = false) @Nullable String cursor,
            @RequestParam(required = false) @Nullable Integer size) {
        int pageSize = CursorPage.clampSize(size);
        JobKey key = jobKey == null ? null : JobKey.of(jobKey);
        List<Execution> fetched = mohs.executions(new ExecutionQuery(key, status, from, to, cursor, pageSize + 1));
        List<ExecutionResponse> responses = fetched.stream().map(ExecutionResponse::from).toList();
        return CursorPage.of(responses, pageSize, ExecutionResponse::executionId);
    }

    @GetMapping("/{id}")
    public ExecutionResponse get(@PathVariable String id) {
        return mohs.findExecution(ExecutionId.of(id)).map(ExecutionResponse::from)
                .orElseThrow(() -> new ExecutionNotFoundException(ExecutionId.of(id)));
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
