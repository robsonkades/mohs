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
package io.mohs.engine;

import java.util.Optional;
import io.mohs.core.definition.DefinitionSource;
import io.mohs.core.definition.JobDefinition;
import io.mohs.core.definition.JobOptions;
import io.mohs.core.job.JobKey;
import io.mohs.core.schedule.Misfire;
import io.mohs.core.schedule.Schedule;

/** Construction and policy projection for typed executable definitions. */
public final class JobDefinitions {
    private JobDefinitions() {}

    /** Projects public policy onto the existing scheduling/execution model. */
    public static JobDefinition definition(JobKey key, JobOptions options, Schedule schedule,
            Misfire misfire, boolean startPaused) {
        return JobDefinition.of(key.value(), schedule, options, misfire, startPaused);
    }

    /** Copies the effective definition policy when creating an independently controlled recurrence. */
    public static JobOptions options(JobDefinition definition) {
        return new JobOptions(definition.name(), definition.runner(), definition.window(), definition.rateLimit(),
                definition.retries(), definition.timeout(), definition.retryPolicy(), definition.maxConcurrentExecutions());
    }
}
