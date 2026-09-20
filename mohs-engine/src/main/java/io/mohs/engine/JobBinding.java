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
package io.mohs.engine;

import java.util.Objects;
import org.jspecify.annotations.Nullable;
import io.mohs.core.job.JobKey;

/** A direct executable binding; a self-binding has no automatic input. */
public record JobBinding(JobKey key, JobKey executableKey, @Nullable JobPayload input) {
    public JobBinding {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(executableKey, "executableKey");
        if (key.equals(executableKey) != (input == null)) {
            throw new IllegalArgumentException("a self-binding has no input; a named schedule requires stored input");
        }
    }
}
