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
 * Mohs's public API — the core contracts.
 *
 * <p>This package carries the facade ({@link io.mohs.core.Mohs}, {@link io.mohs.core.MohsLifecycle},
 * {@link io.mohs.core.EngineState}, {@link io.mohs.core.ScheduleCommand}) and the scheduling receipt
 * ({@link Batch}, {@link BatchBuilder}) — the types that appear only in the facade's own signatures.
 * Shared identity lives in a subpackage of its own ({@code io.mohs.core.job}:
 * {@link io.mohs.core.job.JobKey}, {@link io.mohs.core.job.JobRef}) because it is used by several
 * peer packages (definition, execution, event) that must not depend on one another. The rest of the
 * public vocabulary lives in cohesive subpackages: {@code io.mohs.core.schedule},
 * {@code io.mohs.core.definition}, {@code io.mohs.core.execution}, {@code io.mohs.core.event} and
 * {@code io.mohs.core.resource}.
 *
 * <p>Everything here and in those subpackages is contract — records, sealed interfaces and plain
 * interfaces, with no engine wiring. The real implementation lives in {@code io.mohs.engine} and
 * {@code io.mohs.store.jdbc}, on which no public type may depend — the reactor enforces it: this
 * module has neither on its compile classpath.
 * {@code io.mohs.cron} is a separate utility: it is not job vocabulary, and did not move here.
 *
 * <h2>Compatibility</h2>
 *
 * <p>The interfaces in this API that are NOT {@code sealed} — {@link io.mohs.core.Mohs},
 * {@link io.mohs.core.MohsLifecycle}, {@link io.mohs.core.ScheduleCommand},
 * {@link io.mohs.core.Batch}, {@link io.mohs.core.BatchBuilder},
 * {@link io.mohs.core.execution.JobContext} — cannot be sealed only because the implementation
 * lives in another module ({@code io.mohs.engine}) and the project does not use JPMS to allow a
 * {@code permits} clause across them. They are <b>not extension points</b> and may gain methods in
 * minor releases. Do not write your own implementations of them: to test handlers, use the test kit
 * ({@code io.mohs.test}). Anyone who does so anyway breaks with {@code AbstractMethodError} on the
 * first new method, and this is the warning.
 *
 * <p>The SUPPORTED extension points are {@link io.mohs.core.event.ExecutionListener} and
 * {@link io.mohs.core.event.ExecutionInterceptor}, both {@code @FunctionalInterface} and stable by
 * contract, plus the {@code io.mohs.rest.ActorResolver} SPI in the REST module.
 *
 * <p>{@link io.mohs.core.definition.JobSpec} and the other {@code sealed} interfaces may grow
 * freely: nothing outside this package implements them, so a new method on one is always
 * binary-compatible.
 */
@NullMarked
package io.mohs.core;

import org.jspecify.annotations.NullMarked;
