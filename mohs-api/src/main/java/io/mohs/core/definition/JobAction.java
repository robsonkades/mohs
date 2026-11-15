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
package io.mohs.core.definition;

import io.mohs.core.execution.JobContext;

/**
 * Application code registered once and invoked with independently persisted inputs.
 * Implementations may run concurrently; invocation state belongs in the input or context.
 *
 * @param <T> the input contract
 */
@FunctionalInterface
public interface JobAction<T> {
    /**
     * Executes one attempt on a worker, possibly after restart or redelivery.
     *
     * @param input   the stored invocation input
     * @param context the current attempt and cancellation signal
     * @throws Exception when the attempt fails
     */
    void execute(T input, JobContext context) throws Exception;
}
