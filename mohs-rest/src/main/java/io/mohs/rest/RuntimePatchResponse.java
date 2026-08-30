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
package io.mohs.rest;

import java.util.Objects;

/**
 * The envelope of every {@code PATCH} response — it carries the warning that the change is an
 * emergency one and holds only until the next boot ("runtime
 * PATCH versus boot configuration"), unless {@code mohs.registration.on-conflict: preserve} is
 * configured.
 */
public record RuntimePatchResponse<T>(T resource, String notice) {

    public static final String BOOT_REVERSION_NOTICE =
            "Emergency change: it holds until the next boot; encode it in properties to make it permanent.";

    public RuntimePatchResponse {
        Objects.requireNonNull(resource, "resource");
        Objects.requireNonNull(notice, "notice");
    }

    public static <T> RuntimePatchResponse<T> of(T resource) {
        return new RuntimePatchResponse<>(resource, BOOT_REVERSION_NOTICE);
    }
}
