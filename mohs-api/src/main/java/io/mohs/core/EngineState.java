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

/**
 * The engine's lifecycle state on this node — {@code CREATED -> [STARTING ->] RUNNING <-> PAUSED -> DRAINING ->
 * STOPPED}.
 *
 * <p>Node-local by nature; not to be confused with pausing a job, which is cluster-wide and
 * per-job.
 */
public enum EngineState {
    /**
     * Constructed but not yet started.
     */
    CREATED,
    /** Waiting for the startup delay; no heartbeat or processing has begun. */
    STARTING,
    /**
     * Heartbeating and accepting new claims.
     */
    RUNNING,
    /**
     * Heartbeating while new claims are suspended.
     */
    PAUSED,
    /**
     * Waiting for in-flight work without accepting new claims.
     */
    DRAINING,
    /**
     * Stopped after releasing local execution resources.
     */
    STOPPED
}
