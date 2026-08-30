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
package io.mohs.rest;

import jakarta.servlet.http.HttpServletRequest;

/**
 * Resolves who triggered a mutating action through the REST API — a GoF Strategy: with security
 * plugged in, the authenticated principal; without it (v1), declarative attribution through the
 * {@code X-Mohs-Actor} header (see {@link HeaderActorResolver}). "Who triggered it" is
 * non-negotiable on every invocation.
 *
 * <p>Registering the implementation as a bean is {@code io.mohs.autoconfigure}'s responsibility —
 * this contract only freezes the SPI.
 */
public interface ActorResolver {

    String ANONYMOUS = "anonymous";

    /**
     * Never return {@link io.mohs.core.execution.Execution#SCHEDULER_ACTOR}, in any casing: it is
     * the engine's reserved name — {@code ScheduleCommand.as} rejects it, and rejecting a value
     * coming from this SPI surfaces as a generic 500 rather than the 400 the boundary gives. An
     * authenticated principal carrying that name (a "scheduler" service account, say) must be
     * mapped to another identifier here.
     */
    String resolve(HttpServletRequest request);
}
