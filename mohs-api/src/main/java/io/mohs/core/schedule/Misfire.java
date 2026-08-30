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
package io.mohs.core.schedule;

/**
 * The policy applied when a scheduled firing is missed (a node down, the engine paused, a clock
 * jump). {@link #IGNORE} is the default: a missed firing is simply skipped and the job resumes at
 * the next regular occurrence.
 */
public enum Misfire {

    /** Skips missed firings; the job resumes at the next regular occurrence. The default. */
    IGNORE,

    /** Fires once immediately for the most recent missed occurrence. */
    FIRE_NOW,

    /** Replays every missed occurrence, capped and drained, never discarded. */
    FIRE_ALL_MISSED
}
