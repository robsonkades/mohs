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

/** Queries durable batch counters. Complete submission, completion and cancellation own their writes. */
public interface BatchStore {
    /**
     * @param batchId the batch identity
     * @return the durable counters, or empty when no retained batch exists
     */
    Optional<BatchCounters> find(String batchId);
}
