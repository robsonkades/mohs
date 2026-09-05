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
package io.mohs.rest.batch;

import java.util.Objects;

import io.mohs.core.BatchSnapshot;

/**
 * A batch's aggregate counters — a superset of {@link io.mohs.core.event.BatchCompleted}, which only
 * exists once the batch has finished: here {@code pending = total - succeeded - failed} and the batch
 * may still be {@link BatchState#RUNNING}, since the point of a {@code GET} is to allow polling
 * before completion too.
 */
public record BatchResponse(String batchId, String name, BatchState state, int total, int succeeded, int failed,
        int pending) {

    public BatchResponse {
        Objects.requireNonNull(batchId, "batchId");
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(state, "state");
        if (total < 0 || succeeded < 0 || failed < 0 || pending < 0) {
            throw new IllegalArgumentException("counters must not be negative");
        }
    }

    /**
     * Derives {@code pending} and {@code state} from the three counters — the same static-factory
     * pattern as {@code JobDefinition#of}/{@code ExecutionId#of}.
     *
     * <p>Preferable to the canonical constructor whenever the counters come from a single source
     * (the engine's {@code BatchCounters}, say), which should never be able to disagree with
     * {@code pending} by construction.
     */
    public static BatchResponse of(String batchId, String name, int total, int succeeded, int failed) {
        int pending = total - succeeded - failed;
        BatchState state = pending == 0 ? BatchState.COMPLETED : BatchState.RUNNING;
        return new BatchResponse(batchId, name, state, total, succeeded, failed, pending);
    }

    /** The read model's counters through {@link #of} — the one source that cannot disagree with {@code pending}. */
    static BatchResponse from(BatchSnapshot batch) {
        return of(batch.batchId(), batch.name(), batch.total(), batch.succeeded(), batch.failed());
    }
}
