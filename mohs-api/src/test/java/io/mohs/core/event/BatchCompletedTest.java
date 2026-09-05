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
package io.mohs.core.event;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BatchCompletedTest {

    @Test
    void carriesCounters() {
        BatchCompleted event = new BatchCompleted("import-2026-08", "nightly invoices", 10, 8, 2);

        assertThat(event.batchId()).isEqualTo("import-2026-08");
        assertThat(event.total()).isEqualTo(10);
        assertThat(event.succeeded()).isEqualTo(8);
        assertThat(event.failed()).isEqualTo(2);
    }

    @Test
    void rejectsBlankBatchId() {
        assertThatThrownBy(() -> new BatchCompleted(" ", "n", 0, 0, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsNegativeCounters() {
        assertThatThrownBy(() -> new BatchCompleted("b1", "n", -1, 0, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
