package io.mohs.core;

import java.util.List;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GeneratedIdTest {
    private static final String VALID = "01991337-abcd-7123-8123-012345abcdef";

    @Test
    void identityIsExactAndCanonicalWithoutNormalization() {
        assertThat(GeneratedId.require(VALID, "id")).isSameAs(VALID);
        for (String invalid : List.of("", "execution-1", VALID.toUpperCase(java.util.Locale.ROOT), " " + VALID,
                VALID.replace("-7", "-4"), VALID.replace("-8123-", "-c123-"), VALID.replace('-', '_'), VALID + "0")) {
            assertThatThrownBy(() -> GeneratedId.require(invalid, "id")).isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void paginationAcceptsAnAbsentCursorAndRejectsMalformedIdentities() {
        assertThat(new ExecutionQuery(null, null, null, null, "", 10).cursor()).isNull();
        assertThat(new ExecutionQuery(null, null, null, null, VALID, 10).cursor()).isEqualTo(VALID);
        assertThatThrownBy(() -> new ExecutionQuery(null, null, null, null, "old-id", 10))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
