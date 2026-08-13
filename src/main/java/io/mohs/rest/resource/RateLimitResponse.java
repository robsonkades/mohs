package io.mohs.rest.resource;

import java.time.Duration;
import java.util.Objects;

/** Forma de wire de {@link io.mohs.core.resource.RateLimit}, com o contador runtime de {@code currentCount}. */
public record RateLimitResponse(String name, int max, Duration window, long currentCount) {

    public RateLimitResponse {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(window, "window");
    }
}
