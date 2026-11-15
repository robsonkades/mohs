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
 * Typed scheduling and operational values. {@link io.mohs.core.Mohs} accepts inputs through
 * {@link io.mohs.core.job.JobRef}, using immutable {@link io.mohs.core.SubmissionOptions}.
 * Executable declarations pair a reference with an action in
 * {@link io.mohs.core.definition.JobRegistration}; recurring schedules reference that executable.
 *
 * <p>Receipts are subject to any enclosing database transaction. Recovery and retries may invoke
 * a handler again; effects outside the scheduler database require application idempotency or an
 * external transactional protocol. Listener events are best-effort notifications.
 *
 * <p>Application extension points include {@link io.mohs.core.definition.JobAction},
 * {@link io.mohs.core.event.ExecutionListener}, {@link io.mohs.core.event.ExecutionInterceptor}
 * and {@link io.mohs.core.execution.RetryPolicy}.
 */
@NullMarked
package io.mohs.core;

import org.jspecify.annotations.NullMarked;