package io.mohs.core;

import io.mohs.core.execution.ExecutionId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ExecutionIdTest {

    @Test
    void ofWrapsValue() {
        assertThat(ExecutionId.of("01J4X").value()).isEqualTo("01J4X");
    }

    @ParameterizedTest
    @ValueSource(strings = {"", " ", "\t"})
    void rejectsBlankValue(String blank) {
        assertThatThrownBy(() -> ExecutionId.of(blank))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsNullValue() {
        assertThatThrownBy(() -> new ExecutionId(null))
                .isInstanceOf(NullPointerException.class);
    }
}
