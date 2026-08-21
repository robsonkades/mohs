package io.mohs.engine;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BatchCountersTest {

    @Test
    void pendingIsDerivedFromTheOtherThreeCounters() {
        BatchCounters counters = new BatchCounters("batch-1", 10, 6, 1);

        assertThat(counters.pending()).isEqualTo(3);
    }

    @Test
    void rejectsNullBatchId() {
        assertThatThrownBy(() -> new BatchCounters(null, 0, 0, 0))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void rejectsNegativeTotal() {
        assertThatThrownBy(() -> new BatchCounters("batch-1", -1, 0, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsNegativeSucceeded() {
        assertThatThrownBy(() -> new BatchCounters("batch-1", 0, -1, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsNegativeFailed() {
        assertThatThrownBy(() -> new BatchCounters("batch-1", 0, 0, -1))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
