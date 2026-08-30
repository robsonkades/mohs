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
 * {@code POST /executions/{id}/retry} against an execution that exists but is not {@code FAILED} —
 * a 409: the resource is in a state that conflicts with the requested transition (a manual retry
 * only recognises {@code FAILED}; see {@code Mohs#retry}).
 *
 * <p>The message carries the current state — a 409 should teach, not merely refuse.
 */
public final class ExecutionNotRetryableException extends RuntimeException {

    @Serial
    private static final long serialVersionUID = 1L;

    public ExecutionNotRetryableException(String message) {
        super(message);
    }
}
