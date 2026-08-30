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
import java.util.List;
import java.util.Objects;

import io.mohs.core.job.JobKey;

/**
 * A nonexistent job on a route that expects a valid {@code jobKey}.
 *
 * <p>It carries suggestions by edit distance for
 * {@link io.mohs.rest.error.RestExceptionHandler} to attach to the {@code ProblemDetail} — the
 * promised "404 with nearby jobKey suggestions".
 */
public final class JobNotFoundException extends RuntimeException {

    @Serial
    private static final long serialVersionUID = 1L;

    private final JobKey jobKey;
    private final List<JobKey> nearbyJobKeys;

    public JobNotFoundException(JobKey jobKey, List<JobKey> nearbyJobKeys) {
        super("Job not found: " + jobKey.value());
        this.jobKey = Objects.requireNonNull(jobKey, "jobKey");
        this.nearbyJobKeys = List.copyOf(nearbyJobKeys);
    }

    public JobKey jobKey() {
        return jobKey;
    }

    public List<JobKey> nearbyJobKeys() {
        return nearbyJobKeys;
    }
}
