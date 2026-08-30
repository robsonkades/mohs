/*
 * Copyright 2026 The Mohs Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
 * {@code GET /rate-limits} and {@code PATCH /rate-limits/{name}} — the state and runtime adjustment
 * of throughput, cluster-wide.
 *
 * <p>The {@code PATCH} is the emergency lever on the throughput axis: it applies immediately on every
 * node and survives a restart, but the boot reapplies the code's value on the next start under the
 * default {@code on-conflict: override} — the warning travels in the {@link RuntimePatchResponse}.
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
        // The actor BEFORE the mutation, the same reasoning as the schedule PATCH: a 4xx has to mean
        // "nothing changed", and a mutation without an audit trail is not negotiable.
        String actor = actorResolver.resolve(request);
        RateLimitResponse adjusted = mohs.adjustRateLimit(name, body.max(), body.window())
                .map(RateLimitResponse::from)
                .orElseThrow(() -> new RateLimitNotFoundException(name));
        log.info("rate limit '{}' adjusted at runtime by '{}' to {}/{} — an emergency change, reverting on the next boot under on-conflict=override",
                name, actor, body.max(), body.window());
        return RuntimePatchResponse.of(adjusted);
    }
}
