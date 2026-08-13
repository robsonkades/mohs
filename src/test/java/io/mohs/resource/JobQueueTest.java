package io.mohs.resource;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JobQueueTest {

    @Test
    void defaultsToOne() {
        JobQueue queue = JobQueue.named("emails").build();

        assertThat(queue.name()).isEqualTo("emails");
        assertThat(queue.maxConcurrent()).isEqualTo(1);
    }

    @Test
    void maxConcurrentOverridesTheDefault() {
        JobQueue queue = JobQueue.named("emails").maxConcurrent(10).build();

        assertThat(queue.maxConcurrent()).isEqualTo(10);
    }

    @Test
    void rejectsNonPositiveMaxConcurrent() {
        assertThatThrownBy(() -> JobQueue.named("emails").maxConcurrent(0).build())
                .isInstanceOf(IllegalArgumentException.class);
    }
}
