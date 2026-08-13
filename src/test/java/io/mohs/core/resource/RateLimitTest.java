package io.mohs.core.resource;

import java.time.Duration;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RateLimitTest {

    @Test
    void holdsNameMaxAndWindow() {
        RateLimit rateLimit = new RateLimit("smtp", 100, Duration.ofMinutes(1));

        assertThat(rateLimit.name()).isEqualTo("smtp");
        assertThat(rateLimit.max()).isEqualTo(100);
        assertThat(rateLimit.window()).isEqualTo(Duration.ofMinutes(1));
    }

    @Test
    void rejectsBlankName() {
        assertThatThrownBy(() -> new RateLimit(" ", 100, Duration.ofMinutes(1)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsNonPositiveMax() {
        assertThatThrownBy(() -> new RateLimit("smtp", 0, Duration.ofMinutes(1)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsNonPositiveWindow() {
        assertThatThrownBy(() -> new RateLimit("smtp", 100, Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
