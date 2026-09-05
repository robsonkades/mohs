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
package io.mohs.core.execution;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PriorityTest {

    @Test
    void valuesMatchTheDocumentedClaimWeights() {
        assertThat(Priority.CRITICAL.value()).isZero();
        assertThat(Priority.HIGH.value()).isEqualTo(10);
        assertThat(Priority.NORMAL.value()).isEqualTo(20);
        assertThat(Priority.LOW.value()).isEqualTo(30);
        assertThat(Priority.BACKGROUND.value()).isEqualTo(40);
    }

    @Test
    void lowerValueClaimsFirst() {
        assertThat(Priority.CRITICAL.value()).isLessThan(Priority.HIGH.value());
        assertThat(Priority.HIGH.value()).isLessThan(Priority.NORMAL.value());
        assertThat(Priority.NORMAL.value()).isLessThan(Priority.LOW.value());
        assertThat(Priority.LOW.value()).isLessThan(Priority.BACKGROUND.value());
    }

    /** Every persisted weight resolves back — a guard against drift between the declared weights and the JDBC boundary's lookup. */
    @Test
    void fromValueRoundTripsEveryConstant() {
        for (Priority priority : Priority.values()) {
            assertThat(Priority.fromValue(priority.value())).isSameAs(priority);
        }
    }

    @Test
    void fromValueRejectsAnUnknownWeight() {
        assertThatThrownBy(() -> Priority.fromValue(7))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("7");
    }
}
