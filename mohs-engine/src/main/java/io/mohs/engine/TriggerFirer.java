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

import java.time.Instant;
import java.util.List;

import org.jspecify.annotations.Nullable;

import io.mohs.core.execution.Execution;
import io.mohs.core.job.JobKey;

/**
 * Fires a due trigger — the "due trigger to acquisition" step of the job flow.
 *
 * <p>It is not a Repository for one entity: it advances
 * {@code mohs_job_definitions.next_fire_at} and inserts history ({@code mohs_execution}) plus queue
 * ({@code mohs_ready}) in a single transaction — a port of its own, the same pattern as
 * {@link WorkQueue} ({@code io.mohs.store.jdbc} implements it).
 *
 * <p>The cluster-wide mutual exclusion is the advance's CAS:
 * {@code UPDATE ... WHERE next_fire_at = :observed} — only the node that wins inserts the
 * occurrences, and making the advance and the insert atomic guarantees a crash between them neither
 * loses nor duplicates an occurrence. That is why occurrences carry no Idempotency-Key, which would
 * be subject to the retention window.
 */
public interface TriggerFirer {

    /**
     * CAS-advances {@code next_fire_at} from {@code observedNextFireAt} to {@code newNextFireAt}
     * ({@code null} disarms — fixed-delay awaiting the end) and, on winning, inserts
     * {@code occurrences} with {@code payload} in the same transaction: history
     * ({@code mohs_execution}) plus queue ({@code mohs_ready}), the enqueue unit with the CAS as its
     * guard.
     *
     * <p>{@code now} is the firing instant (and leads history's primary key); each occurrence's
     * {@code scheduledAt} becomes the queue's {@code visible_at}.
     *
     * @param key the stable identity of the job
     * @param observedNextFireAt the trigger instant expected by the compare-and-set
     * @param newNextFireAt the replacement trigger instant, or {@code null} to disarm it
     * @param occurrences the firing occurrences to materialize
     * @param payload the input passed to the job handler
     * @param now the current instant from the configured time source
     * @return {@code true} if THIS call advanced the trigger (and inserted); {@code false} if another
     *         node won the race — nothing was inserted.
     */
    boolean fire(JobKey key, Instant observedNextFireAt, @Nullable Instant newNextFireAt,
            List<Execution> occurrences, Object payload, Instant now);
}
