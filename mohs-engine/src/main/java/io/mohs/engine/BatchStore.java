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

import java.util.Optional;

/**
 * Persistence of batch counters — a Repository (PoEAA), the port {@code io.mohs.store.jdbc}
 * implements.
 *
 * <p>{@link #incrementSucceeded}/{@link #incrementFailed} are atomic increments in SQL, not
 * read-then-write: executions of the same batch complete concurrently on different threads.
 *
 * <p>Both return the POST-increment balance because that is what elects who closes the batch:
 * exactly one caller sees {@link BatchCounters#pending()} at zero, and that is the one firing
 * {@code BatchCompleted}. Asking afterwards, through a separate {@link #find}, would not do — two
 * concurrent completions would read the same final balance and both would believe they closed it.
 */
public interface BatchStore {

    /**
     * {@code name} is the caller's label — persisted, never derived.
     *
     * @param batchId the identity of the batch
     * @param name the human-readable name
     * @param total the total number of batch members
     */
    void insert(String batchId, String name, int total);

    /**
     * Looks up the current batch counters.
     *
     * @param batchId the identity of the batch
     * @return the batch counters, or empty when the batch does not exist
     */
    Optional<BatchCounters> find(String batchId);

    /**
     * Increments the successful-member count for the batch.
     *
     * @param batchId the identity of the batch
     * @return the batch's balance after this increment; {@code pending() == 0} identifies this call
     *         as the one that closed the batch
     */
    BatchCounters incrementSucceeded(String batchId);

    /**
     * Increments the failed-member count for the batch.
     *
     * @param batchId the identity of the batch
     * @return the updated batch counters
     * @see #incrementSucceeded
     */
    BatchCounters incrementFailed(String batchId);
}
