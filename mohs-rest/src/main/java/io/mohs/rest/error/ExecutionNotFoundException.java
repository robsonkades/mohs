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

import io.mohs.core.execution.ExecutionId;

/** A nonexistent execution on a route that expects a valid {@code executionId}/{@code id}. */
public final class ExecutionNotFoundException extends RuntimeException {

    @Serial
    private static final long serialVersionUID = 1L;

    /**
     * Returns the execution identity that was not found.
     */
    private final ExecutionId executionId;

    /**
     * Creates a {@code ExecutionNotFoundException} with the supplied values.
     *
     * @param executionId the identity of the execution
     */
    public ExecutionNotFoundException(ExecutionId executionId) {
        super("Execution not found: " + executionId.value());
        this.executionId = Objects.requireNonNull(executionId, "executionId");
    }

    /**
     * Returns the execution identity that was not found.
     *
     * @return the identity of the execution
     */
    public ExecutionId executionId() {
        return executionId;
    }
}
