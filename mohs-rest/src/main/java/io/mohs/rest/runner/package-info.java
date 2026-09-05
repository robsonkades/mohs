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
 * The "runners" resource area: {@link io.mohs.rest.runner.RunnersController} ({@code GET /runners},
 * read-only — a runner is configuration, not adjustable runtime) and its DTO,
 * {@link io.mohs.rest.runner.RunnerResponse}. Depends on {@code io.mohs.core.resource} (one type
 * reused directly: {@code RunnerMode}).
 */
@NullMarked
package io.mohs.rest.runner;

import org.jspecify.annotations.NullMarked;
