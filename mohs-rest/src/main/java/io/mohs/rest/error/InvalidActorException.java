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

/**
 * The actor declared on the request does not fit the persistence contract — an
 * {@code X-Mohs-Actor} header longer than the {@code actor} column ({@code VARCHAR(255)}), for
 * instance. Validated at the boundary so it becomes a 400 with a detail that teaches, never an
 * {@code INSERT} failure answered as a 500.
 */
public final class InvalidActorException extends RuntimeException {

    @Serial
    private static final long serialVersionUID = 1L;

    /**
     * Creates a {@code InvalidActorException} with the supplied values.
     *
     * @param message the validation message
     */
    public InvalidActorException(String message) {
        super(message);
    }
}
