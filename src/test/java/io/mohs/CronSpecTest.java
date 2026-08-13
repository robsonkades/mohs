package io.mohs;

import org.junit.jupiter.api.Test;

import java.time.ZoneId;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CronSpecTest {

    @Test
    void carriesExpressionAndZone() {
        CronSpec spec = new CronSpec("0 0 2 * * *", ZoneId.of("America/Sao_Paulo"));

        assertThat(spec.expression()).isEqualTo("0 0 2 * * *");
        assertThat(spec.zone()).isEqualTo(ZoneId.of("America/Sao_Paulo"));
    }

    @Test
    void rejectsBlankExpression() {
        assertThatThrownBy(() -> new CronSpec(" ", ZoneId.of("UTC")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsNullZone() {
        assertThatThrownBy(() -> new CronSpec("0 0 2 * * *", null))
                .isInstanceOf(NullPointerException.class);
    }
}
