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

import java.util.Objects;

import io.mohs.core.definition.JobDefinition;

/**
 * The stable identity of a {@link JobDefinition}, used as its persistence key.
 *
 * <p>Unlike {@link JobDefinition#name()}, a {@code JobKey}'s value never changes for a given job:
 * renaming a job's label does not change its key.
 */
public record JobKey(String value) {

    public JobKey {
        Objects.requireNonNull(value, "value");
        if (value.isBlank()) {
            throw new IllegalArgumentException("JobKey value must not be blank");
        }
    }

    /**
     * A static factory rather than a public constructor (Effective Java, Item 1): it reads as a
     * conversion at the call site, e.g. {@code JobKey.of(id)}.
     */
    public static JobKey of(String value) {
        return new JobKey(value);
    }
}
