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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BatchCountersTest {

    @Test
    void pendingIsDerivedFromTheOtherThreeCounters() {
        BatchCounters counters = new BatchCounters("batch-1", "nightly", 10, 6, 1);

        assertThat(counters.pending()).isEqualTo(3);
    }

    @Test
    void rejectsNullBatchId() {
        assertThatThrownBy(() -> new BatchCounters(null, "n", 0, 0, 0))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void rejectsNegativeTotal() {
        assertThatThrownBy(() -> new BatchCounters("batch-1", "n", -1, 0, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsNegativeSucceeded() {
        assertThatThrownBy(() -> new BatchCounters("batch-1", "n", 0, -1, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsNegativeFailed() {
        assertThatThrownBy(() -> new BatchCounters("batch-1", "n", 0, 0, -1))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
