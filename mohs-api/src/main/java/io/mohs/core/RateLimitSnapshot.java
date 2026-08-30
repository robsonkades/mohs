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
package io.mohs.core;

import java.util.Objects;

import io.mohs.core.resource.RateLimit;

/**
 * A {@link RateLimit} plus the current state of the token bucket enforcing it — the read form
 * returned by {@link Mohs#rateLimits}. The same composition as {@link JobSnapshot}: the spec is what
 * was declared, {@code available} is what the engine has right now.
 *
 * @param available tokens available at the instant of the read — how many firings fit NOW, not how
 *        many were used; a full bucket equals {@code rateLimit.max()}
 */
public record RateLimitSnapshot(RateLimit rateLimit, int available) {

    public RateLimitSnapshot {
        Objects.requireNonNull(rateLimit, "rateLimit");
        if (available < 0) {
            throw new IllegalArgumentException("available must not be negative, got " + available);
        }
        if (available > rateLimit.max()) {
            throw new IllegalArgumentException(
                    "available must not exceed the bucket capacity of " + rateLimit.max() + ", got " + available);
        }
    }
}
