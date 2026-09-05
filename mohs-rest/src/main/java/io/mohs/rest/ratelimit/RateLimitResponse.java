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
import java.util.Objects;

import io.mohs.core.RateLimitSnapshot;

/**
 * The wire form of {@link io.mohs.core.resource.RateLimit} plus the bucket's current balance.
 *
 * <p>{@code available} — tokens available — rather than "used": whoever opens the dashboard wants to
 * know how much still fits NOW, and "used" is not even a quantity a bucket has (refill is
 * continuous, with no window boundary at which to reset a counter).
 *
 * @param name the human-readable name
 * @param max the maximum permitted count
 * @param window the positive window over which the rate is limited
 * @param available the available capacity
 */
public record RateLimitResponse(String name, int max, Duration window, int available) {

    /**
     * Creates a {@code RateLimitResponse} with the supplied values.
     *
     * @param name the human-readable name
     * @param max the maximum permitted count
     * @param window the positive window over which the rate is limited
     * @param available the available capacity
     */
    public RateLimitResponse {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(window, "window");
    }

    static RateLimitResponse from(RateLimitSnapshot snapshot) {
        return new RateLimitResponse(snapshot.rateLimit().name(), snapshot.rateLimit().max(),
                snapshot.rateLimit().window(), snapshot.available());
    }
}
