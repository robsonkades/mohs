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
import java.time.Instant;

import org.springframework.lang.CheckReturnValue;

import io.mohs.core.definition.JobDefinition;
import io.mohs.core.event.Enqueued;
import io.mohs.core.execution.Execution;
import io.mohs.core.execution.Priority;

/**
 * A fluent chain over an already existing definition.
 *
 * <p>The pre-terminal steps adjust only the instance — {@code priority}, {@code as},
 * {@code idempotencyKey}; policy (retry, runner) belongs to {@link JobDefinition} and is not
 * overridden here. The terminals ({@code now}/{@code at}/{@code after}) close the chain and persist
 * the execution.
 *
 * <p>{@link CheckReturnValue} turns a chain abandoned before its terminal into a compilation
 * warning rather than runtime silence — the classic builder-without-{@code .build()} bug — and it
 * sits on the NON-terminal steps, which is where an abandoned chain shows up.
 */
public interface ScheduleCommand {

    @CheckReturnValue
    ScheduleCommand priority(Priority priority);

    /**
     * The execution's audit trail ({@code Execution.actor}).
     *
     * @throws IllegalArgumentException if {@code actor} is blank or {@link Execution#SCHEDULER_ACTOR}
     *         — the engine's reserved name: a manual schedule may never pass itself off as a trigger
     *         occurrence
     */
    @CheckReturnValue
    ScheduleCommand as(String actor);

    /**
     * Deduplication by {@code (job, key)} — an Idempotent Receiver: a terminal carrying the same key
     * as an already recorded execution duplicates nothing and returns the original {@link Enqueued}
     * (the same receipt, the same {@code ExecutionId}).
     *
     * <p>The key deduplicates for as long as the execution exists — the window is that of execution
     * retention, which is unbounded while no retention policy exists — so reusing an old key returns
     * the old execution.
     */
    @CheckReturnValue
    ScheduleCommand idempotencyKey(String key);

    // The TERMINALS are deliberately not @CheckReturnValue. The annotation exists against an
    // abandoned chain, and what catches that are the NON-terminal steps plus Mohs#schedule/#batch —
    // all annotated. On a terminal it adds nothing and taxes the single most common line in the
    // whole library: "mohs.schedule(ref, payload).now();" is a correct statement, and a framework
    // whose hello world produces a warning teaches the user to suppress the inspection — at which
    // point they also lose the warnings that matter. Enqueued is a receipt, not a result to inspect.
    Enqueued now();

    Enqueued at(Instant when);

    Enqueued after(Duration delay);
}
