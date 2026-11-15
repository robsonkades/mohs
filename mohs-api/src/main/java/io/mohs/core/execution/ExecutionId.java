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
package io.mohs.core.execution;

import io.mohs.core.GeneratedId;

/**
 * The identity of a single {@link Execution}.
 *
 * <p>Canonical lowercase UUIDv7 text. Its millisecond prefix supports stable keyset ordering;
 * external effects still require their own idempotency protocol.
 *
 * @param value the serialized identity value
 */
public record ExecutionId(String value) {

    /**
     * Creates a {@code ExecutionId} with the supplied values.
     *
     * @param value the serialized identity value
     */
    public ExecutionId {
        GeneratedId.require(value, "executionId");
    }

    /**
     * Creates an execution identity from its serialized value.
     *
     * @param value the serialized identity value
     * @return the validated execution identity
     */
    public static ExecutionId of(String value) {
        return new ExecutionId(value);
    }
}
