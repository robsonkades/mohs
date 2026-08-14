package io.mohs.core.execution;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

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
}
