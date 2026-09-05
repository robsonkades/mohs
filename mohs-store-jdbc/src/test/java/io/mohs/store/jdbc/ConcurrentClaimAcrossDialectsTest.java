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
package io.mohs.store.jdbc;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import javax.sql.DataSource;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.jdbc.core.JdbcTemplate;

import io.mohs.core.execution.ExecutionId;
import io.mohs.core.job.JobKey;
import io.mohs.engine.WorkQueue;
import io.mohs.store.jdbc.delegate.JdbcDelegate;
import io.mohs.store.jdbc.delegate.MySqlJdbcDelegate;
import io.mohs.store.jdbc.delegate.PostgresJdbcDelegate;
import io.mohs.store.jdbc.delegate.SqlServerJdbcDelegate;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Competing Consumers against each real database's own row-lock skipping ({@code SKIP LOCKED} on
 * PostgreSQL and MySQL, {@code READPAST} on SQL Server): eight nodes hammer one shard at once, and
 * every entry ends up claimed by exactly one of them — none twice, none lost. H2 emulates the
 * clause; this is the one place the emulation is not the thing under test.
 */
@Tag("docker")
class ConcurrentClaimAcrossDialectsTest {

    private static final Instant NOW = Instant.parse("2026-08-22T12:00:00Z");
    private static final JobKey JOB = JobKey.of("job-a");
    private static final int ENTRIES = 200;
    private static final int NODES = 8;
    private static final int LIMIT = 10;

    static List<Arguments> dialects() {
        return List.of(
                Arguments.of("postgres", (Supplier<DataSource>) PostgresTestSupport::freshSchema, (Supplier<JdbcDelegate>) PostgresJdbcDelegate::new),
                Arguments.of("mysql", (Supplier<DataSource>) MySqlTestSupport::freshSchema, (Supplier<JdbcDelegate>) MySqlJdbcDelegate::new),
                Arguments.of("sqlserver", (Supplier<DataSource>) SqlServerTestSupport::freshSchema, (Supplier<JdbcDelegate>) SqlServerJdbcDelegate::new));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("dialects")
    void concurrentClaimsHandEveryEntryToExactlyOneNode(String dialect, Supplier<DataSource> freshSchema,
            Supplier<JdbcDelegate> newDelegate) throws Exception {
        DataSource dataSource = freshSchema.get();
        JdbcBatchStore batchStore = new JdbcBatchStore(dataSource, Clock.fixed(NOW, ZoneOffset.UTC), newDelegate.get());
        JdbcWorkQueue queue = new JdbcWorkQueue(dataSource, newDelegate.get(), batchStore);
        queue.offer(IntStream.rangeClosed(1, ENTRIES)
                .mapToObj(i -> new WorkQueue.ReadyEntry(ExecutionId.of("exec-" + i), JOB, 0, 20, 1, NOW.minusSeconds(1)))
                .toList());
        // Every node starts its first claim on the same barrier, so the contention is real from the
        // first statement, not a sequence the executor happened to serialise
        CyclicBarrier allNodesReady = new CyclicBarrier(NODES);
        List<Callable<List<String>>> nodes = new ArrayList<>();
        for (int n = 0; n < NODES; n++) {
            nodes.add(claimingUntilEmpty(queue, "node-" + n, allNodesReady));
        }

        List<String> claimed = new ArrayList<>();
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            // invokeAll is what blocks, so it carries the timeout; a node that never finishes comes
            // back cancelled and its get() throws
            for (Future<List<String>> node : executor.invokeAll(nodes, 60, TimeUnit.SECONDS)) {
                claimed.addAll(node.get());
            }
        }

        assertThat(claimed).as("%s: every entry exactly once", dialect)
                .containsExactlyInAnyOrderElementsOf(IntStream.rangeClosed(1, ENTRIES).mapToObj(i -> "exec-" + i).toList());
        JdbcTemplate jdbc = new JdbcTemplate(dataSource);
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM mohs_ready", Integer.class)).isZero();
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM mohs_lease", Integer.class)).isEqualTo(ENTRIES);
    }

    /** One node's whole run: wait for the others, then claim the shard until it hands back nothing. */
    private static Callable<List<String>> claimingUntilEmpty(JdbcWorkQueue queue, String nodeId, CyclicBarrier allNodesReady) {
        return () -> {
            allNodesReady.await(10, TimeUnit.SECONDS);
            List<String> claimedByThisNode = new ArrayList<>();
            List<WorkQueue.ClaimedWork> batch;
            do {
                batch = queue.claim(0, nodeId, 1, LIMIT, List.of(), NOW);
                batch.forEach(work -> claimedByThisNode.add(work.executionId().value()));
            } while (!batch.isEmpty());
            return claimedByThisNode;
        };
    }
}
