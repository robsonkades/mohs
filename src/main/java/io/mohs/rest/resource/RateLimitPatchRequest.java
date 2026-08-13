package io.mohs.rest.resource;

import java.time.Duration;
import java.util.Objects;

/** Corpo de {@code PATCH /rate-limits/{name}} — mesmo invariante de {@link io.mohs.core.resource.RateLimit}. */
public record RateLimitPatchRequest(int max, Duration window) {

    public RateLimitPatchRequest {
        if (max < 1) {
            throw new IllegalArgumentException("max must be at least 1");
        }
        Objects.requireNonNull(window, "window");
        if (!window.isPositive()) {
            throw new IllegalArgumentException("window must be positive");
        }
    }
}
