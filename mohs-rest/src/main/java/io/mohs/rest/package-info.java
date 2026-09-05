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
 * The operational REST API. It depends only on the public API in {@code io.mohs.core} — never
 * directly on {@code io.mohs.engine} or {@code io.mohs.store.jdbc} — the reactor enforces it:
 * {@code mohs-api} is the only Mohs artifact on this module's compile classpath.
 *
 * <p>This package carries only what is cross-cutting — used by more than one resource subpackage and
 * therefore belonging to none in particular: {@link io.mohs.rest.ActorResolver} (the SPI) and its
 * default implementation {@link io.mohs.rest.HeaderActorResolver}, the pagination envelope
 * {@link io.mohs.rest.CursorPage}, the invocation receipt
 * {@link io.mohs.rest.AcceptedExecutionResponse}, the runtime-adjustment envelope
 * {@link io.mohs.rest.RuntimePatchResponse} and the shared prefix {@link io.mohs.rest.ApiPaths#V1}.
 *
 * <p>It has no controller of its own — each resource area has a subpackage 1:1 with its controller: {@code io.mohs.rest.error} (exception translation into
 * {@code ProblemDetail}, used by all, likewise without a controller),
 * {@code io.mohs.rest.overview}, {@code io.mohs.rest.job}, {@code io.mohs.rest.execution},
 * {@code io.mohs.rest.batch}, {@code io.mohs.rest.ratelimit}, {@code io.mohs.rest.runner} and
 * {@code io.mohs.rest.node}.
 *
 * <h2>DTO suffix convention</h2>
 *
 * <p>{@code *Response} is the default, used both for an endpoint's direct body and for a DTO nested
 * inside another (for example {@link io.mohs.rest.execution.AttemptResponse}, nested in
 * {@code ExecutionResponse.attempts()} — nesting is not the criterion that decides the suffix).
 *
 * <p>{@code *View} is reserved for two cases: the wire adaptation of a {@code sealed} domain type,
 * mirroring its variants 1:1 (for example {@link io.mohs.rest.job.ScheduleView}, mirroring
 * {@link io.mohs.core.schedule.Schedule}), or a computed projection with no corresponding entity in
 * {@code io.mohs.core} (for example {@link io.mohs.rest.overview.ThroughputView} — there is no
 * {@code Throughput} type in the domain). Outside those two cases, every new DTO uses
 * {@code *Response}.
 */
@NullMarked
package io.mohs.rest;

import org.jspecify.annotations.NullMarked;
