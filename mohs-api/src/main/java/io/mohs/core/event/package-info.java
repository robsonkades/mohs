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
 * Observing and extending the execution cycle: the sealed
 * {@link io.mohs.core.event.ExecutionEvent} (8 variants),
 * {@link io.mohs.core.event.ExecutionListener} (Observer),
 * {@link io.mohs.core.event.ExecutionInterceptor} (Chain of Responsibility) and the
 * {@link io.mohs.core.event.OnExecution @OnExecution} sugar. Depends on {@code io.mohs.core.job}
 * ({@code JobKey}) and {@code io.mohs.core.execution} ({@code ExecutionId}; {@code JobContext} only
 * in {@code ExecutionInterceptor}).
 */
@NullMarked
package io.mohs.core.event;

import org.jspecify.annotations.NullMarked;
