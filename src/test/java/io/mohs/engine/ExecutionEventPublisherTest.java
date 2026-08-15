package io.mohs.engine;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;

import org.junit.jupiter.api.Test;
import org.springframework.core.task.AsyncTaskExecutor;

import io.mohs.core.event.Started;
import io.mohs.core.execution.ExecutionId;
import io.mohs.core.job.JobKey;

import static org.assertj.core.api.Assertions.assertThatCode;

class ExecutionEventPublisherTest {

    /** O pipeline de observação nunca exerce backpressure sobre o de controle: executor saturado descarta com WARN — a rejeição jamais sobe pro chamador (tick do Engine ou conclusão do Dispatcher). */
    @Test
    void saturatedExecutorNeverPropagatesToThePublisher() {
        AsyncTaskExecutor saturated = task -> {
            throw new RejectedExecutionException("event executor at its concurrency limit");
        };
        ExecutionEventPublisher publisher = new ExecutionEventPublisher(List.of(event -> {
        }), saturated);

        assertThatCode(() -> publisher.publish(
                new Started(ExecutionId.of("exec-1"), JobKey.of("welcome-email"), 1, Instant.parse("2026-08-15T12:00:00Z"))))
                .doesNotThrowAnyException();
    }
}
