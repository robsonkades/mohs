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

/**
 * A batch's aggregate counters — {@link #pending()} is derived, never persisted. An engine-internal
 * type; {@code io.mohs.rest.batch.BatchResponse} is the equivalent wire form, without depending on
 * this one (the wrong direction).
 */
public record BatchCounters(String batchId, String name, int total, int succeeded, int failed) {

    public BatchCounters {
        Objects.requireNonNull(batchId, "batchId");
        Objects.requireNonNull(name, "name");
        if (total < 0 || succeeded < 0 || failed < 0) {
            throw new IllegalArgumentException("counters must not be negative");
        }
    }

    public int pending() {
        return total - succeeded - failed;
    }
}
