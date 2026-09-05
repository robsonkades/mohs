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
 * A job's identity: {@link io.mohs.core.job.JobKey} (the stable key) and
 * {@link io.mohs.core.job.JobRef} (a typed reference).
 *
 * <p>Extracted apart from {@code io.mohs.core.definition} because identity is shared by peer
 * packages that should not depend on one another — {@code io.mohs.core.definition},
 * {@code io.mohs.core.execution}, {@code io.mohs.core.event} and the facade in
 * {@code io.mohs.core} all depend on this one, not on each other.
 */
@NullMarked
package io.mohs.core.job;

import org.jspecify.annotations.NullMarked;
