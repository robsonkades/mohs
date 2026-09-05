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
 * The execution model: {@link io.mohs.core.execution.Execution},
 * {@link io.mohs.core.execution.Attempt}, {@link io.mohs.core.execution.ExecutionState},
 * {@link io.mohs.core.execution.JobContext} (the handler's optional parameter) and
 * {@link io.mohs.core.execution.Priority}. Depends only on {@code io.mohs.core.job}
 * ({@code JobKey}); {@link io.mohs.core.execution.ExecutionId} lives here — its reach extends no
 * further than {@code io.mohs.core.execution} and {@code io.mohs.core.event}, which depend on this
 * package.
 */
@NullMarked
package io.mohs.core.execution;

import org.jspecify.annotations.NullMarked;
