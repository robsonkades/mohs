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
package io.mohs.core.resource;

import java.util.Objects;

/**
 * The shared "non-null, then non-blank" validation used by {@link MohsRunner},
 * {@link ExecutionWindow} and {@link RateLimit} — the only three classes in this package with a
 * textual {@code name} field.
 *
 * <p>It does not reach beyond {@code io.mohs.core.resource}: the module's other types with the same
 * check ({@code JobKey}, {@code ExecutionId} and so on) live in distinct public packages, where
 * sharing would mean exposing this utility as public API — a higher cost than the two duplicated
 * lines it saves.
 */
final class Fields {

    private Fields() {
    }

    static void requireNotBlank(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
    }
}
