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
package io.mohs.rest.error;

import java.io.Serial;
import java.util.Objects;

/**
 * {@code PATCH /rate-limits/{name}} against a limit that does not exist.
 *
 * <p>Adjusting is an emergency act over something declared; DECLARING is an act of boot. Hence a
 * 404 rather than implicit creation: a limit born from a PATCH would vanish on the next deploy
 * without leaving a trace in the repository.
 */
public final class RateLimitNotFoundException extends RuntimeException {

    @Serial
    private static final long serialVersionUID = 1L;

    /**
     * Returns the rate-limit name that was not found.
     */
    private final String rateLimitName;

    /**
     * Creates a {@code RateLimitNotFoundException} with the supplied values.
     *
     * @param rateLimitName the name of the declared rate limit
     */
    public RateLimitNotFoundException(String rateLimitName) {
        super("Rate limit not found: " + rateLimitName);
        this.rateLimitName = Objects.requireNonNull(rateLimitName, "rateLimitName");
    }

    /**
     * Returns the rate-limit name that was not found.
     *
     * @return the unknown rate-limit name
     */
    public String rateLimitName() {
        return rateLimitName;
    }
}
