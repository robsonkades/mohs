package io.mohs.rest.execution;

import java.net.URI;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import jakarta.servlet.http.HttpServletRequest;

import org.jspecify.annotations.Nullable;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
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
import io.mohs.rest.error.ExecutionNotRetryableException;

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

    /** {@code size} — ver {@link CursorPage#DEFAULT_PAGE_SIZE}/{@link CursorPage#MAX_PAGE_SIZE}. Lista é sumário ({@link ExecutionSummaryResponse}) — attempts moram no detalhe. */
    @GetMapping
    public CursorPage<ExecutionSummaryResponse> search(
            @RequestParam(required = false) @Nullable ExecutionState status,
            @RequestParam(required = false) @Nullable String jobKey,
            @RequestParam(required = false) @Nullable Instant from,
            @RequestParam(required = false) @Nullable Instant to,
            @RequestParam(required = false) @Nullable String cursor,
            @RequestParam(required = false) @Nullable Integer size) {
        int pageSize = CursorPage.clampSize(size);
        JobKey key = jobKey == null ? null : JobKey.of(jobKey);
        List<Execution> fetched = mohs.executions(new ExecutionQuery(key, status, from, to, cursor, pageSize + 1));
        List<ExecutionSummaryResponse> responses = fetched.stream().map(ExecutionSummaryResponse::from).toList();
        return CursorPage.of(responses, pageSize, ExecutionSummaryResponse::executionId);
    }

    @GetMapping("/{id}")
    public ExecutionResponse get(@PathVariable String id) {
        return mohs.findExecution(ExecutionId.of(id)).map(ExecutionResponse::from)
                .orElseThrow(() -> new ExecutionNotFoundException(ExecutionId.of(id)));
    }

    /**
     * Cancelamento é cooperativo, não imediato (ADR-0034) — 202 com o
     * estado atual, não necessariamente terminal: pendente já volta
     * {@code CANCELLED}, {@code RUNNING} volta como está com o pedido
     * registrado. {@code ResponseEntity} (não {@code ExecutionResponse}
     * puro) pelo mesmo motivo de {@link #retry}: é onde o header
     * {@code Location: /executions/{id}} do princípio 1 do design REST é
     * anexado — {@code @ResponseStatus} não teria efeito aqui (REST-1).
     */
    @PostMapping("/{id}/cancel")
    public ResponseEntity<ExecutionResponse> cancel(@PathVariable String id, HttpServletRequest request) {
        ExecutionId executionId = ExecutionId.of(id);
        Execution execution = mohs.cancel(executionId)
                .orElseThrow(() -> new ExecutionNotFoundException(executionId));
        URI location = executionDetailLocation(request, "/cancel");
        return ResponseEntity.accepted().location(location).body(ExecutionResponse.from(execution));
    }

    /**
     * Retry manual (M3 sobre a ADR-0033): rearma a MESMA execução
     * {@code FAILED} como {@code RETRY_WAITING} devida agora — bypassa o
     * orçamento de retries; a nova tentativa disputa o claim como qualquer
     * candidato. Sem {@code Idempotency-Key} de propósito (o header do stub
     * M2 saiu): retry não passa pela dedupe — nada novo é inserido
     * (ADR-0030/0033); a idempotência é o próprio CAS, e repetir o POST
     * vira 409 nomeando o estado atual. Status 202 via
     * {@code ResponseEntity} — {@code @ResponseStatus} não tem efeito sobre
     * {@code ResponseEntity} (REST-1).
     */
    @PostMapping("/{id}/retry")
    public ResponseEntity<AcceptedExecutionResponse> retry(@PathVariable String id, HttpServletRequest request) {
        ExecutionId executionId = ExecutionId.of(id);
        Execution rearmed = retryOrConflict(executionId)
                .orElseThrow(() -> new ExecutionNotFoundException(executionId));
        URI location = executionDetailLocation(request, "/retry");
        return ResponseEntity.accepted().location(location).body(AcceptedExecutionResponse.from(rearmed));
    }

    /**
     * {@code Location: /executions/{id}} deriva da própria URI da requisição
     * (o prefixo já honra {@code mohs.api.base-path}), só removendo o sufixo
     * da ação ({@code /cancel}, {@code /retry}).
     */
    private static URI executionDetailLocation(HttpServletRequest request, String actionSuffix) {
        return URI.create(request.getRequestURI().replaceFirst(actionSuffix + "$", ""));
    }

    /**
     * Toda ISE deste escopo é o guard de estado do retry (contrato
     * documentado em {@code Mohs#retry}) — fora dele, ISE alheia nunca é
     * traduzida pra 409.
     */
    private Optional<Execution> retryOrConflict(ExecutionId executionId) {
        try {
            return mohs.retry(executionId);
        } catch (IllegalStateException e) {
            throw new ExecutionNotRetryableException(Objects.requireNonNullElse(e.getMessage(), e.toString()));
        }
    }
}
