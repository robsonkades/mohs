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
package io.mohs.core.job;

import io.mohs.core.Mohs;

import java.util.Objects;

/**
 * A typed reference to a job definition, binding its {@link JobKey} to the payload type at compile
 * time.
 *
 * <p>Passing a {@code JobRef<WelcomeEmail>} to {@link Mohs#schedule(JobRef, Object)} makes an
 * incompatible payload a compilation error rather than a runtime surprise — the point of preferring
 * typed references over stringly-typed ones.
 */
public record JobRef<T>(JobKey key, Class<T> payloadType) {

    public JobRef {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(payloadType, "payloadType");
    }

    public static <T> JobRef<T> of(String id, Class<T> payloadType) {
        return new JobRef<>(JobKey.of(id), payloadType);
    }
}
