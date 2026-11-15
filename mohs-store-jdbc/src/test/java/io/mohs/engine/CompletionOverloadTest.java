package io.mohs.engine;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessResourceFailureException;
import io.mohs.core.execution.ExecutionId;
import io.mohs.core.execution.ExecutionState;
import io.mohs.core.job.JobKey;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

class CompletionOverloadTest {
    @Test
    void aConcurrentLongerCloseCannotExtendTheFirstDrainDeadline() throws Exception {
        var store = mock(LeaseStore.class);
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        doAnswer(_ -> {
            entered.countDown();
            for (;;) {
                try { if (release.await(5, TimeUnit.SECONDS)) break; }
                catch (InterruptedException ignored) { }
            }
            return Map.of(result(0).executionId(), LeaseStore.Completion.ALREADY_APPLIED);
        }).when(store).complete(anyList());
        var batcher = new CompletionBatcher(store, 1, Duration.ofSeconds(1));
        Thread first = Thread.ofPlatform().unstarted(() -> batcher.close(Duration.ofMillis(500)));
        try (var threads = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            try {
                batcher.start();
                batcher.submit(result(0), _ -> {});
                assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
                first.start();
                long waitUntil = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
                while (first.getState() != Thread.State.TIMED_WAITING && System.nanoTime() - waitUntil < 0) {
                    java.util.concurrent.locks.LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                }
                assertThat(first.getState()).as("the first close is already waiting on its drain deadline")
                        .isEqualTo(Thread.State.TIMED_WAITING);
                var second = threads.submit(() -> batcher.close(Duration.ofSeconds(5)));
                second.get(1500, TimeUnit.MILLISECONDS);
                assertThat(first.join(Duration.ofSeconds(1))).isTrue();
            } finally {
                release.countDown();
                first.join(Duration.ofSeconds(2));
                batcher.close(Duration.ZERO);
            }
        }
    }

    @Test
    void closeDeadlineDoesNotStartSynchronousWritesForTheRemainingQueue() throws Exception {
        var store = mock(LeaseStore.class);
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var finished = new CountDownLatch(1);
        var calls = new AtomicInteger();
        doAnswer(call -> {
            calls.incrementAndGet();
            entered.countDown();
            // A driver may ignore Java interruption. close must still return within its budget.
            boolean released = false;
            while (!released) {
                try { released = release.await(5, TimeUnit.SECONDS); }
                catch (InterruptedException ignored) { }
            }
            finished.countDown();
            List<LeaseStore.CompletionResult> group = call.getArgument(0);
            return Map.of(group.getFirst().executionId(), LeaseStore.Completion.ALREADY_APPLIED);
        }).when(store).complete(anyList());
        var batcher = new CompletionBatcher(store, 1, Duration.ofSeconds(1));
        try (var executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            try {
                batcher.start();
                batcher.submit(result(0), _ -> {});
                assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
                batcher.submit(result(1), _ -> {});
                batcher.submit(result(2), _ -> {});
                var close = executor.submit(() -> batcher.close(Duration.ofMillis(20)));
                close.get(1, TimeUnit.SECONDS);
                assertThat(calls).hasValue(1);
                assertThat(batcher.completionInTransit(result(1).executionId(), result(1).claimToken())).isFalse();
            } finally {
                release.countDown();
                assertThat(finished.await(5, TimeUnit.SECONDS)).isTrue();
                batcher.close();
            }
        }
    }

    @Test
    void persistentOutageDoesNotFanOutIntoOneTransactionPerResult() {
        var store = mock(LeaseStore.class);
        var calls = new AtomicInteger();
        doAnswer(_ -> { calls.incrementAndGet(); throw new DataAccessResourceFailureException("database down"); })
                .when(store).complete(anyList());
        var outcomes = new AtomicInteger();
        var results = IntStream.range(0, 8).mapToObj(CompletionOverloadTest::result).toList();
        try (var batcher = new CompletionBatcher(store, 8, Duration.ofSeconds(1))) {
            results.forEach(result -> batcher.submit(result, _ -> outcomes.incrementAndGet()));
            batcher.start();
            batcher.close();
            assertThat(calls).hasValue(2);
            assertThat(outcomes).hasValue(0);
            results.forEach(result -> assertThat(batcher.completionInTransit(result.executionId(), result.claimToken())).isFalse());
        }
    }

    @Test
    void transientFailureRetriesTheSameGroupAndPublishesItsDurableVerdicts() throws Exception {
        var store = mock(LeaseStore.class);
        var calls = new AtomicInteger();
        var delivered = new CountDownLatch(2);
        doAnswer(call -> {
            List<LeaseStore.CompletionResult> group = call.getArgument(0);
            assertThat(group).hasSize(2);
            if (calls.incrementAndGet() == 1) throw new DataAccessResourceFailureException("connection lost");
            return Map.of(group.get(0).executionId(), LeaseStore.Completion.ALREADY_APPLIED,
                    group.get(1).executionId(), LeaseStore.Completion.owned(group.get(1), null));
        }).when(store).complete(anyList());
        try (var batcher = new CompletionBatcher(store, 2, Duration.ofSeconds(1))) {
            batcher.start();
            batcher.submit(result(0), _ -> delivered.countDown());
            batcher.submit(result(1), _ -> delivered.countDown());
            assertThat(delivered.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(calls).hasValue(2);
        }
    }

    private static LeaseStore.CompletionResult result(int index) {
        Instant now = Instant.parse("2026-09-09T12:00:00Z");
        return new LeaseStore.CompletionResult(ExecutionId.of(io.mohs.test.TestIds.value("overload-" + index)), JobKey.of("job"), "01991337-0000-7000-8000-000000000063", io.mohs.test.TestIds.value("grant-" + index),
                1, now, now, ExecutionState.SUCCEEDED, null, null, null);
    }
}
