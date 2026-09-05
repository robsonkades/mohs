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
 *
 * @param value the serialized identity value
 */
public record JobKey(String value) {

    /**
     * The width of every {@code job_key} column ({@code VARCHAR(255)} on all four dialects). Enforced
     * here so a key that would not fit fails where it is defined, with a message that names the
     * limit — not at the first write, where one dialect raises a driver error and another silently
     * truncates the value and lets two distinct keys collide.
     */
    public static final int MAX_LENGTH = 255;

    /**
     * Creates a {@code JobKey} with the supplied values.
     *
     * @param value the serialized identity value
     */
    public JobKey {
        Objects.requireNonNull(value, "value");
        if (value.isBlank()) {
            throw new IllegalArgumentException("JobKey value must not be blank");
        }
        if (value.length() > MAX_LENGTH) {
            throw new IllegalArgumentException(
                    "JobKey value must be at most " + MAX_LENGTH + " characters, got " + value.length());
        }
    }

    /**
     * A static factory rather than a public constructor (Effective Java, Item 1): it reads as a
     * conversion at the call site, e.g. {@code JobKey.of(id)}.
     *
     * @return the validated job identity
     */
    public static JobKey of(String value) {
        return new JobKey(value);
    }
}
