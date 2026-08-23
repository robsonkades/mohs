package io.mohs.store.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;

import io.mohs.engine.EngineSettings;

/**
 * O arranque do cluster com backlog na frente — o primeiro minuto de todo
 * deploy, e o cenário mais banal que existe: N nós sobem ao mesmo tempo e
 * encontram fila cheia. Nenhum caos, nenhum churn, nenhum nó morrendo.
 *
 * <p>Existe para isolar uma suspeita levantada pelo {@link NodeChurnScenario}:
 * as execuções perdidas lá tinham carimbo de tempo do ARRANQUE, não da
 * saída do nó. A hipótese é a janela entre dois passos do tick de um par —
 * ele lê {@code mohs_nodes} e só depois roda o reaper; um nó que nasce
 * DENTRO dessa janela já reivindicou trabalho enquanto está ausente do
 * snapshot, e "ausente de {@code mohs_nodes} é morto por definição"
 * (ADR-0051). Se a hipótese vale, o par reclama trabalho vivo de um nó
 * saudável, e o fence — que cerca pela posse DO MORTO — não protege.
 *
 * <p>Roda por nome: {@code ./mvnw -pl mohs-benchmark test
 * -Dtest=ColdStartScenario}.
 */
class ColdStartScenario {

    private static final int SEED = 4_000;
    private static final int NODES = 3;
    private static final Duration HANDLER_WORK = Duration.ofMillis(50);
    private static final Duration OBSERVATION = Duration.ofMinutes(2);

    @Test
    void nodesStartingTogetherOnABacklogLoseNothing() {
        DataSource dataSource = PostgresTestSupport.freshSchema();
        Clock clock = Clock.systemUTC();
        AtomicInteger invocations = new AtomicInteger();

        try (ScenarioCluster cluster = new ScenarioCluster(dataSource, clock)) {
            cluster.defineJob("cold", _ -> {
            });
            for (int i = 0; i < NODES; i++) {
                cluster.addNode(settings(), List.of());
            }
            cluster.registerEverywhere("cold", (_, _) -> {
                invocations.incrementAndGet();
                Thread.sleep(HANDLER_WORK);
            });

            cluster.seedReady("cold", SEED, 20);
            cluster.startAll();

            boolean settled = ScenarioCluster.awaitUntil(OBSERVATION, cluster::isDrained);

            int succeeded = cluster.countTerminal("SUCCEEDED");
            int failed = cluster.countTerminal("FAILED");

            report(settled, invocations.get(), succeeded, failed, cluster.failureKinds());

            assertThat(settled)
                    .as("the queue must drain — a stalled queue is a different bug from a lost execution, "
                            + "and the assertions below cannot tell them apart")
                    .isTrue();
            assertThat(failed)
                    .as("nothing may be lost to a plain cluster start — no node died, no handler threw")
                    .isZero();
            assertThat(succeeded).isEqualTo(SEED);
        }
    }

    private static EngineSettings settings() {
        return new EngineSettings(Duration.ofMillis(25), Duration.ofMillis(200), 64, 32, 3,
                Duration.ofSeconds(30), Duration.ofSeconds(15), null, Duration.ofSeconds(60));
    }

    private static void report(boolean settled, int invocations, int succeeded, int failed,
            Map<String, Integer> failureKinds) {
        System.out.printf("""

                === Cold start — %d nodes starting together on a %d backlog ===
                queue settled        : %s
                handler invocations  : %d
                terminal SUCCEEDED   : %d
                terminal FAILED      : %d
                failure kinds        : %s
                """, NODES, SEED, settled ? "yes" : "NO — still draining at the timeout", invocations,
                succeeded, failed, failureKinds);
    }
}
