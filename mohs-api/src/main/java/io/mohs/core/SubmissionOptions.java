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

import io.mohs.core.execution.Priority;
import org.jspecify.annotations.Nullable;

import java.util.Objects;

/**
 * Immutable options for one execution. Copy methods never submit work.
 *
 * @param priority       claim priority
 * @param actor          application audit identity
 * @param idempotencyKey optional deduplication key scoped to the job, retained independently of history
 */
public record SubmissionOptions(Priority priority, String actor, @Nullable String idempotencyKey) {
    /**
     * Creates and validates this immutable value.
     *
     * @param priority       claim priority
     * @param actor          application audit identity
     * @param idempotencyKey optional deduplication key scoped to the job, retained independently of history
     */
    public SubmissionOptions {
        Objects.requireNonNull(priority, "priority");
        Objects.requireNonNull(actor, "actor");
        if (actor.isBlank() || actor.length() > 255) {
            throw new IllegalArgumentException("actor must be nonblank and at most 255 characters");
        }
        if (idempotencyKey != null) TechnicalKey.require(idempotencyKey, "idempotencyKey");
    }

    /**
     * Normal priority, application actor, no deduplication.
     *
     * @return the default options
     */
    public static SubmissionOptions defaults() {
        return new SubmissionOptions(Priority.NORMAL, "application", null);
    }

    /**
     * Returns a copy with the supplied claim priority.
     *
     * @param priority claim priority
     * @return a new value with the requested change
     */
    public SubmissionOptions withPriority(Priority priority) {
        return new SubmissionOptions(priority, actor, idempotencyKey);
    }

    /**
     * Returns a copy with the supplied audit identity.
     *
     * @param actor application audit identity
     * @return a new value with the requested change
     */
    public SubmissionOptions withActor(String actor) {
        return new SubmissionOptions(priority, actor, idempotencyKey);
    }

    /**
     * Returns a copy with a non-null deduplication key.
     *
     * @param key the nonblank deduplication key
     * @return a new value with the requested change
     */
    public SubmissionOptions withIdempotencyKey(String key) {
        return new SubmissionOptions(priority, actor, Objects.requireNonNull(key, "key"));
    }
}
