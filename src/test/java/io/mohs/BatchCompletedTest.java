package io.mohs;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BatchCompletedTest {

    @Test
    void carriesCounters() {
        BatchCompleted event = new BatchCompleted("import-2026-08", 10, 8, 2);

        assertThat(event.batchId()).isEqualTo("import-2026-08");
        assertThat(event.total()).isEqualTo(10);
        assertThat(event.succeeded()).isEqualTo(8);
        assertThat(event.failed()).isEqualTo(2);
    }

    @Test
    void rejectsBlankBatchId() {
        assertThatThrownBy(() -> new BatchCompleted(" ", 0, 0, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsNegativeCounters() {
        assertThatThrownBy(() -> new BatchCompleted("b1", -1, 0, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
