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

import java.util.Objects;

/**
 * A batch's read model — what {@code GET /batches/{id}} returns and what {@link Mohs#findBatch}
 * hands to a caller polling it. The same role {@link RateLimitSnapshot} plays for a limit: a public
 * read type, separate from the internal counter the engine increments.
 *
 * <p>{@link #pending()} is derived, not stored: a fourth column could drift from the other three,
 * and there is no question it would answer any faster.
 *
 * <p>{@link #name()} is the label the caller gave in {@link Mohs#batch}, which used to be required
 * and then discarded — whoever opened the dashboard at 3 a.m. found a UUID where they had written
 * {@code "nightly-invoices"}. It is persisted and derived from nothing: it is the only way for an
 * operator to tie the batch back to the intent.
 */
public record BatchSnapshot(String batchId, String name, int total, int succeeded, int failed) {

    public BatchSnapshot {
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

    /** Members that have not yet reached a terminal state. */
    public int pending() {
        return total - succeeded - failed;
    }

    /** {@code true} when no member is pending — a batch does not reopen. */
    public boolean completed() {
        return pending() == 0;
    }
}
