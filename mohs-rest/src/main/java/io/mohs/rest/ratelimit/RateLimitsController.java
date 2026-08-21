package io.mohs.rest.ratelimit;

import java.util.List;
import java.util.Objects;

import jakarta.servlet.http.HttpServletRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import io.mohs.core.Mohs;
import io.mohs.rest.ActorResolver;
import io.mohs.rest.ApiPaths;
import io.mohs.rest.RuntimePatchResponse;
import io.mohs.rest.error.RateLimitNotFoundException;

/**
 * {@code GET /rate-limits} · {@code PATCH /rate-limits/{name}} — estado e
 * ajuste runtime de vazão, cluster-wide (ADR-0042). O {@code PATCH} é a
 * alavanca de emergência sobre o eixo de vazão: vale imediatamente em
 * todos os nós e sobrevive a restart, mas o boot reaplica o valor do
 * código no próximo start sob o default {@code on-conflict: override} —
 * o aviso viaja no {@link RuntimePatchResponse}.
 */
@RestController
@RequestMapping("${mohs.api.base-path:" + ApiPaths.V1 + "}/rate-limits")
public class RateLimitsController {

    private static final Logger log = LoggerFactory.getLogger(RateLimitsController.class);

    private final Mohs mohs;
    private final ActorResolver actorResolver;

    public RateLimitsController(Mohs mohs, ActorResolver actorResolver) {
        this.mohs = Objects.requireNonNull(mohs, "mohs");
        this.actorResolver = Objects.requireNonNull(actorResolver, "actorResolver");
    }

    @GetMapping
    public List<RateLimitResponse> list() {
        return mohs.rateLimits().stream().map(RateLimitResponse::from).toList();
    }

    @PatchMapping("/{name}")
    public RuntimePatchResponse<RateLimitResponse> patch(@PathVariable String name, @RequestBody RateLimitPatchRequest body,
            HttpServletRequest request) {
        // actor ANTES da mutação, mesmo raciocínio do PATCH de agenda
        // (ADR-0010/0036): 4xx tem que significar "nada mudou", e mutação sem
        // trilha de auditoria não é negociável.
        String actor = actorResolver.resolve(request);
        RateLimitResponse adjusted = mohs.adjustRateLimit(name, body.max(), body.window())
                .map(RateLimitResponse::from)
                .orElseThrow(() -> new RateLimitNotFoundException(name));
        log.info("rate limit '{}' adjusted at runtime by '{}' to {}/{} — emergency change (ADR-0042), reverts on next boot under on-conflict=override",
                name, actor, body.max(), body.window());
        return RuntimePatchResponse.of(adjusted);
    }
}
