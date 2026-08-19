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

    /**
     * ADR-0042: o balde rende um token a cada {@code window / max}. Se essa
     * divisão trunca para zero, o refill dividiria por {@code Duration.ZERO}
     * e derrubaria a rodada de claim INTEIRA — inclusive os jobs sem limite
     * nenhum. A spec recusa nascer nesse estado, então nem property, nem
     * {@code @Bean}, nem PATCH conseguem produzi-lo.
     */
    @Test
    void rejectsAWindowTooShortToIssueOneTokenPerInterval() {
        assertThatThrownBy(() -> new RateLimit("smtp", 2_000_000, Duration.ofMillis(1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("too short for max");
    }

    /** O limite exato — um token por nanossegundo — continua válido: a rejeição é do que não é representável, não do que é agressivo. */
    @Test
    void acceptsAWindowThatIssuesExactlyOneTokenPerNanosecond() {
        assertThat(new RateLimit("smtp", 1_000_000, Duration.ofMillis(1)).max()).isEqualTo(1_000_000);
    }
}
