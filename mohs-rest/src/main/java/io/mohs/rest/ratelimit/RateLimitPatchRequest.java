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

import java.time.Duration;

import io.mohs.core.resource.RateLimit;

/**
 * The body of {@code PATCH /rate-limits/{name}} — it validates through the SAME rule as
 * {@link RateLimit#requireRefillable}, not a copy: divergence between the wire validation and the
 * domain's is what would let a too-short {@code window} cross the API and bring down the claim
 * round. Here the failure is a 422 in the client's face; at boot it is a startup error.
 *
 * @param max the maximum permitted count
 * @param window the positive window over which the rate is limited
 */
public record RateLimitPatchRequest(int max, Duration window) {

    /**
     * Creates a {@code RateLimitPatchRequest} with the supplied values.
     *
     * @param max the maximum permitted count
     * @param window the positive window over which the rate is limited
     */
    public RateLimitPatchRequest {
        RateLimit.requireRefillable(max, window);
    }
}
