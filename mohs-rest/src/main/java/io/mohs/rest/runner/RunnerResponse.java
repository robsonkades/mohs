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
package io.mohs.rest.runner;

import java.util.Objects;

import io.mohs.core.RunnerSnapshot;
import io.mohs.core.resource.RunnerMode;

/**
 * The wire form of {@link io.mohs.core.resource.MohsRunner} — a node-local view by nature (see
 * "mode, max, running").
 *
 * <p>{@code max} is derived from the mode: {@code maxConcurrent} for {@link RunnerMode#IO},
 * {@code maxSize} for {@link RunnerMode#CPU}. The remaining CPU-only tuning fields
 * ({@code coreSize}/{@code queueCapacity}/{@code keepAlive}) are not exposed here — only what the
 * design document asks for.
 */
public record RunnerResponse(String name, RunnerMode mode, int max, int running) {

    public RunnerResponse {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(mode, "mode");
    }

    public static RunnerResponse from(RunnerSnapshot snapshot) {
        Objects.requireNonNull(snapshot, "snapshot");
        return new RunnerResponse(snapshot.name(), snapshot.mode(), snapshot.max(), snapshot.running());
    }
}
