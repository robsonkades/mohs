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

    @Test
    void rejectsNullValue() {
        assertThatThrownBy(() -> new JobKey(null))
                .isInstanceOf(NullPointerException.class);
    }
}
