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

import java.time.Clock;
import java.time.Duration;

/**
 * A {@link Clock} that must be resampled periodically against an external time source (a database,
 * say, in "database" mode).
 *
 * <p>Whoever builds the {@code Clock} ({@code io.mohs.autoconfigure}) owns calling {@link #sync()}
 * at the configured interval ({@code mohs.time.sync-interval}) — this contract exists so that it
 * need not know the concrete type ({@code DatabaseClock}, in {@code io.mohs.store.jdbc}) nor carry
 * any JDBC dependency.
 */
public interface SyncableClock {

    /** One sample: it measures the offset against the external source and applies the monotonic clamp. */
    void sync();

    /** The current offset (external source minus application), exposed for when the metrics infrastructure exists. */
    Duration currentOffset();
}
