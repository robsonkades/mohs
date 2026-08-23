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
 * O primeiro instante de todo deploy em Kubernetes: N réplicas sobem ao
 * MESMO tempo contra um banco vazio, e cada uma chama
 * {@link MohsFlyway#migrate()} no boot (ADR-0048 — as migrações são da
 * biblioteca, não do host). Ninguém coordena a ordem; o orquestrador sobe
 * os pods em paralelo de propósito.
 *
 * <p>O que se afirma: exatamente UMA réplica aplica cada versão, nenhuma
 * falha o boot, e o schema resultante é o mesmo de uma migração solitária.
 * O contrário — duas réplicas aplicando o mesmo DDL — é `CrashLoopBackOff`
 * no melhor caso e schema meio-aplicado no pior.
 *
 * <p>A largada é um {@link CountDownLatch}, não um {@code sleep}: o valor
 * do cenário está em todas as réplicas chegarem ao {@code migrate()} na
 * mesma janela de microssegundos.
 *
 * <p>Roda por nome: {@code ./mvnw -pl mohs-benchmark test
 * -Dtest=ConcurrentMigrationScenario}.
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

        // I/O-bound (espera no lock de migração do Postgres) → virtual threads,
        // nomeadas, como manda o CLAUDE.md
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
                // sem isto, uma réplica travada no lock faria o close() do
                // try-with-resources esperar UM DIA em vez de falhar o build
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
        // isNotEmpty ANTES de doesNotHaveDuplicates: "nenhum duplicado" sobre
        // lista vazia é verdade e não significa nada — seria exatamente o
        // veredito verde que um migrate() virado no-op produziria
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
