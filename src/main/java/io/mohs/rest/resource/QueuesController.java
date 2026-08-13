package io.mohs.rest.resource;

import java.util.List;

import jakarta.servlet.http.HttpServletRequest;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import io.mohs.rest.RuntimePatchResponse;

/**
 * {@code GET /queues} · {@code PATCH /queues/{name}} — estado e ajuste
 * runtime de {@code maxConcurrent}, cluster-wide (ver
 * {@code docs/REST-API-DESIGN.md}, "PATCH runtime × configuração de boot").
 */
@RestController
@RequestMapping("/api/mohs/v1/queues")
public class QueuesController {

    @GetMapping
    public List<QueueResponse> list() {
        throw new UnsupportedOperationException("M3: ainda não implementado");
    }

    @PatchMapping("/{name}")
    public RuntimePatchResponse<QueueResponse> patch(
            @PathVariable String name, @RequestBody QueuePatchRequest body, HttpServletRequest request) {
        throw new UnsupportedOperationException("M3: ainda não implementado");
    }
}
