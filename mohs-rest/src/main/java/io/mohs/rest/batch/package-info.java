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
/**
 * The "batches" resource area: {@link io.mohs.rest.batch.BatchesController}
 * ({@code GET /batches/{id}}) and its DTOs — {@link io.mohs.rest.batch.BatchResponse} and
 * {@link io.mohs.rest.batch.BatchState}.
 *
 * <p>A package of its own, 1:1 with the controller, even though {@code BatchCompleted} lives inside
 * {@code io.mohs.core.event} alongside the other execution events on the domain side — the
 * granularity here follows the controller, not the core's grouping.
 */
@NullMarked
package io.mohs.rest.batch;

import org.jspecify.annotations.NullMarked;
