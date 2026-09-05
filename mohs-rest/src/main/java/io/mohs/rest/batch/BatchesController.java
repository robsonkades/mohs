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

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import io.mohs.core.Mohs;
import io.mohs.rest.ApiPaths;
import io.mohs.rest.error.BatchNotFoundException;

/** {@code GET /batches/{id}} — a batch's aggregate counters and state. */
@RestController
@RequestMapping("${mohs.api.base-path:" + ApiPaths.V1 + "}/batches")
public class BatchesController {

    private final Mohs mohs;

    public BatchesController(Mohs mohs) {
        this.mohs = Objects.requireNonNull(mohs, "mohs");
    }

    /**
     * A seek on the primary key, flat in the batch's size — the maintained counter is what pays for
     * this route being cheap.
     *
     * <p>{@code pending} and {@code state} are derived from the three counters by
     * {@link BatchResponse#from} rather than stored: one more column could drift from the others, and
     * there is no question it would answer any faster.
     */
    @GetMapping("/{id}")
    public BatchResponse get(@PathVariable String id) {
        return mohs.findBatch(id)
                .map(BatchResponse::from)
                .orElseThrow(() -> new BatchNotFoundException(id));
    }
}
