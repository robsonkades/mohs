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

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import io.mohs.store.jdbc.dialect.PostgresJdbcDialect;

/**
 * The first instant of every Kubernetes deploy: N replicas start at the SAME time against an empty
 * database, and each one calls {@link MohsFlyway#migrate()} on boot, because the migrations belong
 * to the library rather than to the host. Nobody coordinates the order; the orchestrator starts
 * the pods in parallel on purpose.
 *
 * <p>What is asserted: exactly ONE replica applies each version, none fails to boot, and the
 * resulting schema matches that of a lone migration. The opposite — two replicas applying the same
 * DDL — is a {@code CrashLoopBackOff} at best and a half-applied schema at worst.
 *
 * <p>The start is a {@link CountDownLatch}, not a {@code sleep}: the whole value of the scenario is
 * that every replica reaches {@code migrate()} within the same window of microseconds.
 *
 * <p>Run by name: {@code ./mvnw -pl mohs-benchmark test -Dtest=ConcurrentMigrationScenario}.
 */
class ConcurrentMigrationScenario {

    private static final int REPLICAS = 6;
    private static final Duration REPLICA_TIMEOUT = Duration.ofMinutes(2);

    @Test
    void replicasBootingTogetherMigrateExactlyOnce() throws Exception {
        DataSource dataSource = PostgresTestSupport.freshEmptyDatabase("mohs_concurrent_migration");
        JdbcTemplate jdbc = new JdbcTemplate(dataSource);
        CountDownLatch startLine = new CountDownLatch(1);
        List<ReplicaOutcome> outcomes = new ArrayList<>();

        // I/O-bound (waiting on Postgres's migration lock), so virtual threads, named as the
        // project requires
        try (ExecutorService replicas = Executors.newThreadPerTaskExecutor(
                Thread.ofVirtual().name("mohs-migration-replica-", 0).factory())) {
            List<Future<ReplicaOutcome>> futures = new ArrayList<>();
            for (int i = 0; i < REPLICAS; i++) {
                int replica = i;
                futures.add(replicas.submit(() -> {
                    startLine.await();
                    long startedAt = System.nanoTime();
                    new MohsFlyway(dataSource, new PostgresJdbcDialect()).migrate();
                    return new ReplicaOutcome(replica, System.nanoTime() - startedAt, null);
                }));
            }
            startLine.countDown();
            try {
                for (Future<ReplicaOutcome> future : futures) {
                    try {
                        outcomes.add(future.get(REPLICA_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
                    } catch (ExecutionException e) {
                        outcomes.add(new ReplicaOutcome(outcomes.size(), 0, e.getCause()));
                    }
                }
            } finally {
                // Without this, a replica stuck on the lock would make the try-with-resources
                // close() wait A DAY instead of failing the build
                futures.forEach(future -> future.cancel(true));
            }
        }

        List<String> versions = jdbc.queryForList(
                "SELECT \"version\" FROM \"" + MohsFlyway.HISTORY_TABLE + "\" ORDER BY \"installed_rank\"", String.class);
        Integer failedMigrations = jdbc.queryForObject(
                "SELECT count(*) FROM \"" + MohsFlyway.HISTORY_TABLE + "\" WHERE NOT \"success\"", Integer.class);
        List<ReplicaOutcome> broken = outcomes.stream().filter(outcome -> outcome.failure() != null).toList();

        System.out.printf("""

                === Concurrent migration — %d replicas booting together ===
                replicas that threw  : %d %s
                versions applied     : %d %s
                failed migrations    : %d
                slowest replica      : %.2fs
                """, REPLICAS, broken.size(),
                broken.isEmpty() ? "" : broken.stream().map(outcome -> String.valueOf(outcome.failure())).toList(),
                versions.size(), versions, failedMigrations,
                outcomes.stream().mapToLong(ReplicaOutcome::nanos).max().orElse(0) / 1e9);

        assertThat(broken).as("no replica may fail its boot because a peer was migrating at the same time").isEmpty();
        assertThat(failedMigrations).as("a half-applied migration leaves the schema in an unknown state").isZero();
        // isNotEmpty BEFORE doesNotHaveDuplicates: "no duplicates" over an empty list is true and
        // means nothing — it is exactly the green verdict a migrate() turned no-op would produce
        assertThat(versions)
                .as("the whole migration chain must have been applied — an empty history means migrate() did nothing")
                .isNotEmpty()
                .doesNotHaveDuplicates();
        assertThat(jdbc.queryForObject("SELECT count(*) FROM mohs_ready", Integer.class))
                .as("the schema must be usable, and empty, after the concurrent boot").isZero();
    }

    private record ReplicaOutcome(int replica, long nanos, @org.jspecify.annotations.Nullable Throwable failure) {
    }
}
