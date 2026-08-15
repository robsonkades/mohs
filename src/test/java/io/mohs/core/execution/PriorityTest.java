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

    /** Todo peso persistido resolve de volta — guarda contra drift entre os pesos declarados e o lookup da borda JDBC. */
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
