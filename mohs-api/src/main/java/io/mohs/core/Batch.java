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

import java.util.function.Consumer;

import io.mohs.core.event.BatchCompleted;
import io.mohs.core.event.ExecutionListener;

/**
 * A batch's receipt — {@link #batchId()} is already available synchronously, with the same
 * durability the async contract promises for an enqueue.
 *
 * <p>{@link #onCompletion} registers a best-effort continuation, in the same spirit as
 * {@link ExecutionListener}: a guaranteed reaction enqueues a job inside its own transaction rather
 * than depending on this callback.
 */
public interface Batch {

    /**
     * Returns the identity used to look up this batch.
     *
     * @return the stable batch identity
     */
    String batchId();

    /**
     * Registers a best-effort callback fired at the end of the batch, on success or failure — this
     * is not the batch's completion guarantee, only a convenience notification.
     *
     * <p>Each call registers an independent listener without replacing those already registered:
     * {@code batch.onCompletion(a).onCompletion(b)} registers both, not just the last. The
     * {@link Batch} returned references the same batch, never a copy.
     *
     * @param callback the callback to invoke when the batch completes
     * @return this batch receipt
     */
    Batch onCompletion(Consumer<BatchCompleted> callback);
}
