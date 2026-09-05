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
import io.mohs.core.job.JobKey;

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

    /**
     * Sets the claim priority for this invocation.
     *
     * @param priority the ordering priority used when claiming work
     * @return this configuration stage for further customization
     */
    @CheckReturnValue
    ScheduleCommand priority(Priority priority);

    /**
     * The execution's audit trail ({@code Execution.actor}).
     *
     * @param actor the identity attributed to the operation
     * @return this configuration stage for further customization
     * @throws IllegalArgumentException if {@code actor} is blank or {@link Execution#SCHEDULER_ACTOR}
     *         — the engine's reserved name: a manual schedule may never pass itself off as a trigger
     *         occurrence
     */
    @CheckReturnValue
    ScheduleCommand as(String actor);

    /**
     * The widest key {@link #idempotencyKey} accepts: the width of the column that stores it, on
     * every dialect — the very ceiling {@link JobKey#MAX_LENGTH} enforces for the job key, because
     * the two columns are declared with one width. Refused at the call rather than at the write —
     * one dialect answers an oversized value with a driver error, and MySQL without strict mode
     * truncates it, so two keys sharing a prefix would collide and the second schedule would be
     * deduplicated away.
     */
    int MAX_IDEMPOTENCY_KEY_LENGTH = JobKey.MAX_LENGTH;

    /**
     * Deduplication by {@code (job, key)} — an Idempotent Receiver: a terminal carrying the same key
     * as an already recorded execution duplicates nothing and returns the original {@link Enqueued}
     * (the same receipt, the same {@code ExecutionId}).
     *
     * <p>The key deduplicates while its idempotency record is retained, independently of execution
     * history retention. Reusing a retained key returns the original identity even if its history
     * has been pruned; after the key is pruned, the same value can schedule a new execution.
     *
     * @param key the deduplication key scoped to this job
     * @return this configuration stage for further customization
     * @throws IllegalArgumentException if the key is blank, or longer than
     *         {@link #MAX_IDEMPOTENCY_KEY_LENGTH} — a blank key is never an intent to deduplicate,
     *         and accepted it would collapse every keyless-by-mistake schedule of the job into one
     */
    @CheckReturnValue
    ScheduleCommand idempotencyKey(String key);

    // The TERMINALS are deliberately not @CheckReturnValue. The annotation exists against an
    // abandoned chain, and what catches that are the NON-terminal steps plus Mohs#schedule/#batch —
    // all annotated. On a terminal it adds nothing and taxes the single most common line in the
    // whole library: "mohs.schedule(ref, payload).now();" is a correct statement, and a framework
    // whose hello world produces a warning teaches the user to suppress the inspection — at which
    // point they also lose the warnings that matter. Enqueued is a receipt, not a result to inspect.
    /**
     * Enqueues the invocation for immediate eligibility.
     *
     * @return the enqueue receipt, subject to commit of any enclosing transaction
     */
    Enqueued now();

    /**
     * Enqueues the invocation for the supplied instant.
     *
     * @param when the instant at which the execution becomes eligible
     * @return the enqueue receipt, subject to commit of any enclosing transaction
     */
    Enqueued at(Instant when);

    /**
     * Enqueues the invocation after the supplied delay.
     *
     * @param delay the delay before the scheduled instant
     * @return the enqueue receipt, subject to commit of any enclosing transaction
     */
    Enqueued after(Duration delay);
}
