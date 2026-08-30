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
 * Parsing and next-occurrence calculation for seconds-first cron expressions, including the Quartz
 * L/W/# extensions — vendored from {@code org.springframework.scheduling.support} (Spring Framework,
 * Apache License 2.0; see each file's header for the exact attribution and what was adapted).
 *
 * <p>Why vendor rather than depend on Spring directly: the supporting classes
 * ({@code CronField}, {@code BitsCronField}, {@code CompositeCronField}, {@code QuartzCronField})
 * are package-private there — only {@code CronExpression} is public — so using the real parser, not
 * just the facade, requires having the code rather than only the dependency.
 *
 * <p>{@link io.mohs.cron.CronExpression} is a self-contained utility: it knows nothing of
 * {@code io.mohs.core.schedule.CronSpec}, {@code io.mohs.core.definition.JobDefinition} or any job
 * vocabulary. Stitching the raw string from {@code CronSpec.expression()} to the next-occurrence
 * calculation is the engine's responsibility, not this package's.
 */
@NullMarked
package io.mohs.cron;

import org.jspecify.annotations.NullMarked;
