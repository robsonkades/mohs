package io.mohs.rest.job;

import java.util.List;

import jakarta.servlet.http.HttpServletRequest;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import io.mohs.rest.AcceptedExecutionResponse;
import io.mohs.rest.CursorPage;
import io.mohs.rest.execution.ExecutionResponse;

/**
 * Área de recurso "jobs" (ver {@code docs/REST-API-DESIGN.md}). Definição
 * de job é código, não API — não há {@code POST}/{@code PUT}/{@code DELETE}
 * aqui, só leitura e invocação sobre definição existente.
 */
@RestController
@RequestMapping("/api/mohs/v1/jobs")
public class JobsController {

    @GetMapping
    public List<JobResponse> list() {
        throw new UnsupportedOperationException("M3: ainda não implementado");
    }

    @GetMapping("/{jobKey}")
    public JobResponse get(@PathVariable String jobKey) {
        throw new UnsupportedOperationException("M3: ainda não implementado");
    }

    @PostMapping("/{jobKey}/schedule")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public ResponseEntity<AcceptedExecutionResponse> schedule(@PathVariable String jobKey, @RequestBody ScheduleJobRequest body, @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey, HttpServletRequest request) {
        throw new UnsupportedOperationException("M3: ainda não implementado");
    }

    @PostMapping("/{jobKey}/pause")
    public JobResponse pause(@PathVariable String jobKey, HttpServletRequest request) {
        throw new UnsupportedOperationException("M3: ainda não implementado");
    }

    @PostMapping("/{jobKey}/resume")
    public JobResponse resume(@PathVariable String jobKey, HttpServletRequest request) {
        throw new UnsupportedOperationException("M3: ainda não implementado");
    }

    @GetMapping("/{jobKey}/executions")
    public CursorPage<ExecutionResponse> executions(@PathVariable String jobKey, @RequestParam(required = false) String cursor) {
        throw new UnsupportedOperationException("M3: ainda não implementado");
    }
}
