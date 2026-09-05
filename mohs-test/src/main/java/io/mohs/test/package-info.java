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
 * Test kit for consumers of the library to exercise their own job handlers.
 *
 * <p>{@link io.mohs.test.MutableClock} is the clock a test injects wherever the engine reads
 * "now" — the one behind {@code mohs.clock()} in the test kit.
 * {@link io.mohs.test.InMemoryJobStore} is the in-memory implementation of
 * {@link io.mohs.engine.JobStore}, drawing the same line between a job's definition and its
 * operational state as {@code JdbcJobStore} (io.mohs.store.jdbc), with no database involved.
 */
@NullMarked
package io.mohs.test;

import org.jspecify.annotations.NullMarked;
