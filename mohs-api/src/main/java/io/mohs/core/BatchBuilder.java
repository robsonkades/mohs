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
package io.mohs.core;

import io.mohs.core.job.JobRef;

/** The accumulator of a flat batch — each {@link #add} schedules one job inside the same batch. */
@FunctionalInterface
public interface BatchBuilder {
    /**
     * Adds one invocation to the batch being assembled.
     *
     * @param <T> the payload type
     * @param ref the typed reference identifying the job and its payload contract
     * @param payload the input passed to the job handler
     */
    <T> void add(JobRef<T> ref, T payload);
}
