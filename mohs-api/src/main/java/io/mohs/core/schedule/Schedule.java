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
package io.mohs.core.schedule;

import io.mohs.core.definition.JobDefinition;
import io.mohs.core.definition.JobSpec;

/**
 * When a job fires: cron, fixed-rate, fixed-delay or on demand.
 *
 * <p>Sealed so that the engine can switch exhaustively over the three variants — adding a fourth
 * kind of schedule is a compilation error at every use site until it is handled, rather than a
 * silent {@code default} branch.
 *
 * <p>This type carries only the trigger. A job's policies (runner, window, misfire, retries,
 * timeout) live in {@link JobDefinition}; see {@link JobSpec} for the staged builder that assembles
 * both.
 */
public sealed interface Schedule permits CronSpec, IntervalSpec, OnDemandSpec {
}
