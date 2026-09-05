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
import java.util.function.Supplier;

/**
 * A payload incompatible with the type the job's definition expects — it names the offending field
 * ("incompatible becomes a 422 problem+json naming the field").
 */
public final class PayloadValidationException extends RuntimeException {

    @Serial
    private static final long serialVersionUID = 1L;

    private final String field;

    public PayloadValidationException(String field, String message) {
        super(Objects.requireNonNull(message, "message"));
        this.field = Objects.requireNonNull(field, "field");
    }

    /**
     * Runs a validation that refuses with an {@code IllegalArgumentException} (a value object's
     * constructor, a command's guard) and turns that refusal into this 422 naming {@code field}.
     * Only an IAE thrown INSIDE {@code validation} is translated — the caller keeps the scope of the
     * supplier tight enough that every IAE in it is validation by construction.
     */
    public static <T> T validating(String field, Supplier<T> validation) {
        try {
            return validation.get();
        } catch (IllegalArgumentException e) {
            throw new PayloadValidationException(field, Objects.requireNonNullElse(e.getMessage(), e.toString()));
        }
    }

    public String field() {
        return field;
    }
}
