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
package io.mohs.core.event;

import java.util.Objects;

import io.mohs.core.Batch;

/**
 * A deliberate dual role, in the same spirit as {@link Enqueued}: it is both the data handed to
 * {@link Batch#onCompletion} and the corresponding {@link ExecutionEvent} variant — which avoids
 * duplicating the same shape under two names.
 *
 * <p>It carries the batch's {@code name}: a batch-completion callback that does not know WHICH
 * batch finished is half a callback, and anyone registering {@link Batch#onCompletion} on more than
 * one batch had to match UUIDs by hand.
 *
 * @param batchId the identity of the batch
 * @param name the human-readable name
 * @param total the total number of batch members
 * @param succeeded the number of successful terminal executions
 * @param failed the number of failed terminal executions
 */
public record BatchCompleted(String batchId, String name, int total, int succeeded, int failed) implements ExecutionEvent {

    /**
     * Creates a {@code BatchCompleted} with the supplied values.
     *
     * @param batchId the identity of the batch
     * @param name the human-readable name
     * @param total the total number of batch members
     * @param succeeded the number of successful terminal executions
     * @param failed the number of failed terminal executions
     */
    public BatchCompleted {
        Objects.requireNonNull(batchId, "batchId");
        if (batchId.isBlank()) {
            throw new IllegalArgumentException("batchId must not be blank");
        }
        Objects.requireNonNull(name, "name");
        if (name.isBlank()) {
            throw new IllegalArgumentException("name must not be blank");
        }
        if (total < 0 || succeeded < 0 || failed < 0) {
            throw new IllegalArgumentException("counters must not be negative");
        }
        if (succeeded + failed > total) {
            throw new IllegalArgumentException("succeeded + failed must not exceed total");
        }
    }
}
