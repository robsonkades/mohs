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
package io.mohs.core;

import java.time.Duration;

/**
 * The engine's lifecycle on this node — not to be confused with pausing a job, which is
 * cluster-wide and per-job, through {@link Mohs} or REST.
 */
public interface MohsLifecycle {

    EngineState state();

    void start();

    void pause();

    void resume();

    /** Stops accepting claims and waits for in-flight work for up to {@code grace}. A drain is not a cancel. */
    void drain(Duration grace);

    /** {@link #drain} followed by shutting the runners down. */
    void stop(Duration grace);
}
