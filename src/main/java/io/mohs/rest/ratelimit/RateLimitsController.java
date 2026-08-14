package io.mohs.rest.ratelimit;

import java.util.List;

import jakarta.servlet.http.HttpServletRequest;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import io.mohs.rest.ApiPaths;
import io.mohs.rest.RuntimePatchResponse;

/** {@code GET /rate-limits} · {@code PATCH /rate-limits/{name}} — estado e ajuste runtime de vazão, cluster-wide. */
@RestController
@RequestMapping(ApiPaths.V1 + "/rate-limits")
public class RateLimitsController {

    @GetMapping
    public List<RateLimitResponse> list() {
        throw new UnsupportedOperationException("M3: ainda não implementado");
    }

    @PatchMapping("/{name}")
    public RuntimePatchResponse<RateLimitResponse> patch(
            @PathVariable String name, @RequestBody RateLimitPatchRequest body, HttpServletRequest request) {
        throw new UnsupportedOperationException("M3: ainda não implementado");
    }
}
