/*
 * Copyright 2026 The Mohs Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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

    /** The observation pipeline never exerts backpressure on the control one: a saturated executor drops with a WARN — the rejection never climbs to the caller (the Engine's tick or the Dispatcher's completion). */
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
