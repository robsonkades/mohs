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

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.mohs.core.event.BatchCompleted;
import io.mohs.core.event.Succeeded;
import io.mohs.core.execution.ExecutionId;
import io.mohs.core.job.JobKey;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The continuation registry behind {@code Batch.onCompletion}: delivery is the listener contract
 * (best-effort, one failing observer never silences the others), a registration is consumed by the
 * event that closes its batch, and the map is bounded by an LRU because a batch closed on ANOTHER
 * node never removes its entry here.
 */
class BatchCompletionCallbacksTest {

    private final BatchCompletionCallbacks callbacks = new BatchCompletionCallbacks();

    private static BatchCompleted closed(String batchId) {
        return new BatchCompleted(batchId, "nightly", 2, 2, 0);
    }

    @Test
    void everyCallbackOfTheClosedBatchRunsOnceAndTheRegistrationIsConsumed() {
        List<String> delivered = new ArrayList<>();
        callbacks.register("b1", event -> delivered.add("first:" + event.batchId()));
        callbacks.register("b1", event -> delivered.add("second:" + event.batchId()));
        callbacks.register("b2", event -> delivered.add("other:" + event.batchId()));

        callbacks.on(closed("b1"));
        callbacks.on(closed("b1"));

        assertThat(delivered).containsExactly("first:b1", "second:b1");
    }

    /** One broken observer must not take the others down: the failure is logged and the remaining callbacks still run. */
    @Test
    void aCallbackThatThrowsDoesNotStopTheOnesAfterIt() {
        List<String> delivered = new ArrayList<>();
        callbacks.register("b1", _ -> {
            throw new IllegalStateException("observer broke");
        });
        callbacks.register("b1", event -> delivered.add("survivor:" + event.batchId()));

        callbacks.on(closed("b1"));

        assertThat(delivered).containsExactly("survivor:b1");
    }

    /**
     * Best-effort, this-JVM-only, and the registration does not know whether its batch is still
     * open: a callback registered after the batch closed on this node never runs — it sits in the
     * map until the LRU evicts it. Anyone needing a guaranteed reaction enqueues a job inside the
     * batch's transaction instead.
     */
    @Test
    void aCallbackRegisteredAfterTheBatchClosedNeverRuns() {
        List<String> delivered = new ArrayList<>();
        callbacks.on(closed("b1"));

        callbacks.register("b1", event -> delivered.add(event.batchId()));

        assertThat(delivered).isEmpty();
    }

    @Test
    void anEventOfAnotherKindIsIgnored() {
        List<String> delivered = new ArrayList<>();
        callbacks.register("b1", event -> delivered.add(event.batchId()));

        callbacks.on(new Succeeded(ExecutionId.of("exec-1"), JobKey.of("welcome-email"), 1));

        assertThat(delivered).isEmpty();
    }

    /**
     * The ceiling is honest degradation: past {@code MAX_TRACKED_BATCHES} live registrations, the
     * least recently touched one is dropped and its batch closes without a callback — the newer
     * ones are unaffected.
     */
    @Test
    void pastTheCeilingTheLeastRecentlyTouchedRegistrationIsEvicted() {
        List<String> delivered = new ArrayList<>();
        callbacks.register("oldest", event -> delivered.add(event.batchId()));
        for (int i = 0; i < BatchCompletionCallbacks.MAX_TRACKED_BATCHES; i++) {
            callbacks.register("b" + i, event -> delivered.add(event.batchId()));
        }

        callbacks.on(closed("oldest"));
        callbacks.on(closed("b0"));

        assertThat(delivered).containsExactly("b0");
    }

    /** LRU by access, not by insertion: a batch that keeps receiving registrations stays resident. */
    @Test
    void reRegisteringOnTheOldestBatchKeepsItResident() {
        List<String> delivered = new ArrayList<>();
        callbacks.register("oldest", event -> delivered.add("a:" + event.batchId()));
        for (int i = 0; i < BatchCompletionCallbacks.MAX_TRACKED_BATCHES - 1; i++) {
            callbacks.register("b" + i, event -> delivered.add(event.batchId()));
        }
        callbacks.register("oldest", event -> delivered.add("b:" + event.batchId()));
        callbacks.register("newcomer", event -> delivered.add(event.batchId()));

        callbacks.on(closed("oldest"));
        callbacks.on(closed("b0"));

        assertThat(delivered).containsExactly("a:oldest", "b:oldest");
    }
}
