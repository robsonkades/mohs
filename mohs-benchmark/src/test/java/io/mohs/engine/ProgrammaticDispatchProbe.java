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

import io.mohs.core.job.JobKey;

/** Lets the opt-in I/O harness exercise the exact dispatcher resolution path. */
public final class ProgrammaticDispatchProbe {
    private ProgrammaticDispatchProbe() {}

    public static JobHandler resolve(JobKey key, JobStore store, HandlerRegistry registry) {
        return store.find(key).flatMap(job -> registry.find(job.definition().executableKey())).orElseThrow();
    }
}
