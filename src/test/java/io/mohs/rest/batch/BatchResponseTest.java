package io.mohs.rest.batch;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class BatchResponseTest {

    @Test
    void ofDerivesPendingAndRunningState() {
        BatchResponse response = BatchResponse.of("batch-1", 10, 6, 1);

        assertThat(response.pending()).isEqualTo(3);
        assertThat(response.state()).isEqualTo(BatchState.RUNNING);
    }

    @Test
    void ofDerivesCompletedStateWhenNothingIsPending() {
        BatchResponse response = BatchResponse.of("batch-1", 10, 8, 2);

        assertThat(response.pending()).isZero();
        assertThat(response.state()).isEqualTo(BatchState.COMPLETED);
    }
}
