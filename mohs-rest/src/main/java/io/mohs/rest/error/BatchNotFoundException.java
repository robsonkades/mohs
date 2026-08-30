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
import java.util.Objects;

/** A nonexistent batch on a route that expects a valid {@code batchId}. */
public final class BatchNotFoundException extends RuntimeException {

    @Serial
    private static final long serialVersionUID = 1L;

    private final String batchId;

    public BatchNotFoundException(String batchId) {
        super("Batch not found: " + batchId);
        this.batchId = Objects.requireNonNull(batchId, "batchId");
    }

    public String batchId() {
        return batchId;
    }
}
