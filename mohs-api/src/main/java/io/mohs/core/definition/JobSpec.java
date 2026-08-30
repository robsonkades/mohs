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
package io.mohs.core.definition;

import java.time.Duration;
import java.time.ZoneId;

/**
 * The staged builder for programmatic job definitions (see {@link JobDefinition#of}).
 *
 * <p>Choosing a trigger — {@link #cron}, {@link #every}, {@link #everyAfterFinish} or
 * {@link #onDemand} — is the first step and returns {@link PolicySpec}, which does not expose those
 * methods again: the compiler makes "cron and every" unrepresentable, instead of leaving it to a
 * validation error at boot.
 *
 * <p>Sealed to a single implementation on purpose (a staged Builder): it preserves binary
 * compatibility for new methods in minor releases, since nothing outside this package can implement
 * it.
 */
public sealed interface JobSpec permits JobSpecImpl {

    PolicySpec cron(String expression, ZoneId zone);

    PolicySpec every(Duration interval);

    PolicySpec everyAfterFinish(Duration interval);

    PolicySpec onDemand();
}
