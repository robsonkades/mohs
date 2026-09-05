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

import io.mohs.core.job.JobKey;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JobKeyTest {

    @Test
    void ofWrapsValue() {
        assertThat(JobKey.of("welcome-email").value()).isEqualTo("welcome-email");
    }

    @ParameterizedTest
    @ValueSource(strings = {"", " ", "\t"})
    void rejectsBlankValue(String blank) {
        assertThatThrownBy(() -> JobKey.of(blank))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** The column is 255 wide on every dialect, and MySQL without strict mode would truncate rather than refuse — so the key refuses first, naming the limit. */
    @Test
    void rejectsAValueWiderThanTheColumn() {
        assertThat(JobKey.of("k".repeat(JobKey.MAX_LENGTH)).value()).hasSize(255);
        assertThatThrownBy(() -> JobKey.of("k".repeat(JobKey.MAX_LENGTH + 1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("255")
                .hasMessageContaining("256");
    }

    @Test
    void rejectsNullValue() {
        assertThatThrownBy(() -> new JobKey(null))
                .isInstanceOf(NullPointerException.class);
    }
}
