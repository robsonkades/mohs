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

import java.util.Objects;

/**
 * The identity of a single {@link Execution}.
 *
 * <p>Opaque by design — the engine decides the concrete format when it starts generating them in
 * {@code io.mohs.store.jdbc}; this contract only requires a non-empty value.
 */
public record ExecutionId(String value) {

    public ExecutionId {
        Objects.requireNonNull(value, "value");
        if (value.isBlank()) {
            throw new IllegalArgumentException("ExecutionId value must not be blank");
        }
    }

    public static ExecutionId of(String value) {
        return new ExecutionId(value);
    }
}
