package io.mohs;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IntervalSpecTest {

    @Test
    void acceptsPositiveInterval() {
        IntervalSpec spec = new IntervalSpec(Duration.ofSeconds(30), false);

        assertThat(spec.interval()).isEqualTo(Duration.ofSeconds(30));
        assertThat(spec.afterFinish()).isFalse();
    }

    @Test
    void rejectsZeroInterval() {
        assertThatThrownBy(() -> new IntervalSpec(Duration.ZERO, false))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsNegativeInterval() {
        assertThatThrownBy(() -> new IntervalSpec(Duration.ofSeconds(-1), false))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
