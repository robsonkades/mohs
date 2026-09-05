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
 * The "rate-limits" resource area: {@link io.mohs.rest.ratelimit.RateLimitsController}
 * ({@code GET}/{@code PATCH /rate-limits/{name}}, cluster-wide) and its
 * DTOs — {@link io.mohs.rest.ratelimit.RateLimitResponse},
 * {@link io.mohs.rest.ratelimit.RateLimitPatchRequest}. Depends on
 * {@code io.mohs.rest} ({@code RuntimePatchResponse}).
 */
@NullMarked
package io.mohs.rest.ratelimit;

import org.jspecify.annotations.NullMarked;
