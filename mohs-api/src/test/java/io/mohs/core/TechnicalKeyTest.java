package io.mohs.core;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TechnicalKeyTest {
    @Test
    void exactAsciiIdentityIsNotTrimmedOrCaseFolded() {
        assertThat(TechnicalKey.require("Group/a_B-9.v1:Job", "key")).isEqualTo("Group/a_B-9.v1:Job");
        assertThat(TechnicalKey.require("Alpha", "key")).isNotEqualTo(TechnicalKey.require("alpha", "key"));
        for (String invalid : new String[]{"", " leading", "trailing ", "has space", "ação", "a\u0000b", "x".repeat(256)}) {
            assertThatThrownBy(() -> TechnicalKey.require(invalid, "key")).isInstanceOf(IllegalArgumentException.class);
        }
        assertThat(TechnicalKey.require("x".repeat(255), "key")).hasSize(255);
    }
}
