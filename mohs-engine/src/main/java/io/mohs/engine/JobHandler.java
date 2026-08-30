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

import io.mohs.core.execution.JobContext;

/**
 * An already resolved handler, ready to call — it decouples {@link Dispatcher} from how a
 * {@link io.mohs.core.job.JobKey} becomes something invocable. Scanning for {@code @MohsJob} and
 * resolving the Spring bean that owns the method is {@code io.mohs.autoconfigure}'s work; this port
 * only assumes that, by the time it is called, that resolution has happened.
 */
@FunctionalInterface
public interface JobHandler {

    void invoke(Object payload, JobContext ctx) throws Exception;
}
