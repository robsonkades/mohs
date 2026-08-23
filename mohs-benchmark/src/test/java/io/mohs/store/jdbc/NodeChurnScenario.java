package io.mohs.store.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;

import io.mohs.core.job.JobKey;
import io.mohs.engine.EngineSettings;
import io.mohs.engine.JobHandler;

/**
 * O deploy do dia a dia: cluster com backlog, um nó sai por
 * {@code stop(grace)} (o que um SIGTERM de orquestrador provoca) e um nó
 * novo entra no lugar no meio do drain. Nada aqui é caos — é terça-feira.
 *
 * <p>O que se afirma:
 * <ul>
 *   <li>nenhuma execução se perde — todas terminam, e em {@code SUCCEEDED};</li>
 *   <li>a re-entrega é BOUNDED: só o que estava em voo quando o nó saiu
 *       pode rodar duas vezes (at-least-once, ADR-0003), e o
 *       {@code stop(grace)} existe justamente para que nem isso aconteça —
 *       o dano esperado com folga de grace é ZERO;</li>
 *   <li>os shards do nó que saiu voltam a ser reivindicados: se a
 *       atribuição derivada (ADR-0055) não reagisse à membership, a fila
 *       pararia com trabalho pronto na frente — e é isso que o
 *       {@code settled} pega.</li>
 * </ul>
 *
 * <p>Roda por nome: {@code ./mvnw -pl mohs-benchmark test
 * -Dtest=NodeChurnScenario}.
 */
class NodeChurnScenario {

    private static final int SEED = 8_000;
    private static final Duration OBSERVATION = Duration.ofMinutes(2);
    /** Handler lento de propósito: sem trabalho em voo no instante da saída, o cenário não testaria o drain. */
    private static final Duration HANDLER_WORK = Duration.ofMillis(60);
    /** Posse mínima antes de derrubar o nó: sair de mãos vazias mediria um cluster de 2 nós e chamaria isso de churn. */
    private static final int IN_FLIGHT_BEFORE_EXIT = 20;
    /** A folga entre drain e stop: a variável independente do experimento, não sincronização preguiçosa. */
    private static final Duration SETTLE_BEFORE_STOP = Duration.ofSeconds(1);
    /**
     * Um retry além da primeira tentativa. É o que separa as duas leituras
     * possíveis do achado: com orçamento, o reclaim de um órfão vira attempt
     * sintético + renascimento na fila (reentrega BOUNDED — o contrato
     * at-least-once da ADR-0003, que é o que este cenário diz medir); sem
     * orçamento, o MESMO mecanismo vira FAILED terminal por configuração, e
     * a bancada chamaria de "perda" o que o produto chama de "reentrega".
     * A aresta de {@code retries=0} tem teste próprio abaixo.
     */
    private static final int RETRIES = 1;

    @Test
    void aNodeLeavingAndAnotherJoiningMidDrainLosesNothing() {
        DataSource dataSource = PostgresTestSupport.freshSchema();
        Clock clock = Clock.systemUTC();
        Map<String, Integer> invocationsPerExecution = new ConcurrentHashMap<>();
        AtomicInteger invocations = new AtomicInteger();
        JobHandler handler = (_, ctx) -> {
            invocations.incrementAndGet();
            invocationsPerExecution.merge(ctx.executionId().value(), 1, Integer::sum);
            Thread.sleep(HANDLER_WORK);
        };

        try (ScenarioCluster cluster = new ScenarioCluster(dataSource, clock)) {
            cluster.defineJob("churn", spec -> spec.retries(RETRIES));
            for (int i = 0; i < 3; i++) {
                cluster.addNode(settings(), List.of());
            }
            cluster.registerEverywhere("churn", handler);

            cluster.seedReady("churn", SEED, 20);
            cluster.startAll();

            // o nó tem de sair com trabalho NA MÃO: espera a posse encher
            ScenarioCluster.awaitUntil(Duration.ofSeconds(30),
                    () -> cluster.countLease() >= IN_FLIGHT_BEFORE_EXIT);
            int inFlightAtExit = cluster.countLease();

            // a posse POR NÓ tirada antes da saída: o bound de re-execução é
            // sobre o que o nó QUE SAI tinha na mão, e countLease() global é
            // ~3× mais frouxo do que a mensagem anuncia. Qual das linhas é a
            // dele só se sabe depois — o nó que saiu é o único STOPPED.
            Map<String, Integer> leasesByNodeAtExit = leasesByNode(cluster);
            ScenarioCluster.Node leaving = cluster.nodes().get(2);
            long exitAt = System.nanoTime();
            leaving.engine().stop(Duration.ofSeconds(20));
            long exitNanos = System.nanoTime() - exitAt;
            String leftNodeId = stoppedNodeId(cluster);
            int heldByLeavingNode = leasesByNodeAtExit.getOrDefault(leftNodeId, 0);

            ScenarioCluster.Node joining = cluster.addNode(settings(), List.of());
            joining.handlers().register(JobKey.of("churn"), handler);
            joining.engine().start();

            boolean settled = ScenarioCluster.awaitUntil(OBSERVATION, cluster::isDrained);

            int succeeded = cluster.countTerminal("SUCCEEDED");
            int failed = cluster.countTerminal("FAILED");
            long reExecuted = invocationsPerExecution.values().stream().filter(count -> count > 1).count();

            report(settled, inFlightAtExit, exitNanos, invocations.get(), invocationsPerExecution.size(), succeeded,
                    failed, reExecuted, cluster.failureKinds());
            if (failed > 0) {
                dumpLostWork(cluster);
            }

            assertThat(settled).as("the leaving node's shards must be re-claimed — a stalled queue means the assignment did not react").isTrue();
            // com orçamento de retry, um órfão reclamado RENASCE na fila: o
            // attempt sintético aparece em failureKinds (por isso ele não é
            // exigido vazio), mas a EXECUÇÃO tem de acabar bem. A causa entra
            // na mensagem para o vermelho já vir atribuído, sem depender de
            // alguém ler o stdout.
            assertThat(failed)
                    .as("a graceful exit must not lose work — with retries=%d a reclaimed orphan is redelivered, "
                            + "not failed. Causes seen: %s", RETRIES, cluster.failureKinds())
                    .isZero();
            assertThat(succeeded).as("every seeded execution must end SUCCEEDED").isEqualTo(SEED);
            assertThat(reExecuted)
                    .as("re-execution must be bounded by what the LEAVING node held (%d) — that is the at-least-once "
                            + "contract of ADR-0003; a graceful drain should leave zero", heldByLeavingNode)
                    .isLessThanOrEqualTo(heldByLeavingNode);
        }
    }

    /**
     * O braço de contraste, e a ÚNICA variável que muda em relação ao teste
     * acima é {@code drain} + folga antes do {@code stop} — o nó novo entra
     * do mesmo jeito, no mesmo ponto. Sem essa disciplina o par não
     * distinguiria a hipótese do {@code stop} da hipótese do nó nascente (a
     * mesma TOCTOU que o {@link ColdStartScenario} caça: {@code findAll} e
     * {@code findOrphaned} são leituras separadas dentro de um tick).
     *
     * <p>A hipótese: {@code stop} chama {@code drain}, que espera o que está
     * em {@code inFlight} NAQUELE instante; o lote que o tick em curso já
     * reivindicou e ainda não submeteu entra depois ({@code Engine} registra
     * no {@code inFlight} DEPOIS do {@code runAsync}), e o heartbeat final
     * — lease vencida por decisão da ADR-0051 — declara o nó morto com esse
     * lote ainda rodando. Dar ao nó um tempo em {@code DRAINING}, onde o
     * tick não reivindica mais nada, deve zerar a perda.
     */
    @Test
    void drainingBeforeStoppingLosesNothing() throws Exception {
        DataSource dataSource = PostgresTestSupport.freshSchema();
        Clock clock = Clock.systemUTC();
        AtomicInteger invocations = new AtomicInteger();
        JobHandler handler = (_, _) -> {
            invocations.incrementAndGet();
            Thread.sleep(HANDLER_WORK);
        };

        try (ScenarioCluster cluster = new ScenarioCluster(dataSource, clock)) {
            cluster.defineJob("churn", spec -> spec.retries(RETRIES));
            for (int i = 0; i < 3; i++) {
                cluster.addNode(settings(), List.of());
            }
            cluster.registerEverywhere("churn", handler);
            cluster.seedReady("churn", SEED, 20);
            cluster.startAll();

            ScenarioCluster.awaitUntil(Duration.ofSeconds(30),
                    () -> cluster.countLease() >= IN_FLIGHT_BEFORE_EXIT);
            ScenarioCluster.Node leaving = cluster.nodes().get(2);
            leaving.engine().drain(Duration.ofSeconds(20));
            Thread.sleep(SETTLE_BEFORE_STOP.toMillis()); // a folga que a hipótese diz faltar dentro do stop
            leaving.engine().stop(Duration.ofSeconds(20));

            ScenarioCluster.Node joining = cluster.addNode(settings(), List.of());
            joining.handlers().register(JobKey.of("churn"), handler);
            joining.engine().start();

            boolean settled = ScenarioCluster.awaitUntil(OBSERVATION, cluster::isDrained);
            int succeeded = cluster.countTerminal("SUCCEEDED");
            int failed = cluster.countTerminal("FAILED");

            System.out.printf("""

                    === Node churn — drain, settle, then stop (seed %d) ===
                    queue settled        : %s
                    handler invocations  : %d
                    terminal SUCCEEDED   : %d
                    terminal FAILED      : %d
                    failure kinds        : %s
                    """, SEED, settled ? "yes" : "NO", invocations.get(), succeeded, failed, cluster.failureKinds());

            assertThat(settled).as("the queue must drain — a stall is a different bug from a loss").isTrue();
            assertThat(failed).as("a drain that is allowed to settle must lose nothing").isZero();
            assertThat(succeeded).isEqualTo(SEED);
        }
    }

    /**
     * A aresta afiada, afirmada no valor que ela TEM: o mesmo
     * {@code stop(grace)}, com o default de {@code retries} do produto —
     * ZERO ({@code JobSpecImpl.retries} nasce 0). Aqui o reclaim do órfão
     * não tem para onde reagendar e vira {@code FAILED} terminal: o que
     * seria reentrega vira perda. Não é um defeito diferente do teste
     * acima — é o MESMO mecanismo, com a política que decide se ele custa
     * uma re-execução ou uma execução perdida.
     *
     * <p>Está aqui para que a escolha apareça: quem roda com
     * {@code retries=0} paga isto em todo SIGTERM que pegue trabalho em voo.
     */
    @Test
    void aGracefulStopWithNoRetryBudgetLosesTheWorkItWasHolding() {
        DataSource dataSource = PostgresTestSupport.freshSchema();
        Map<String, Integer> invocationsPerExecution = new ConcurrentHashMap<>();
        AtomicInteger invocations = new AtomicInteger();
        // handler IDÊNTICO ao do primeiro teste, merge no mapa incluído: os
        // dois só podem diferir em `retries`, senão o par não isola nada —
        // e o merge por execução muda o tempo dentro do handler, que é
        // justamente a janela que o experimento observa
        JobHandler handler = (_, ctx) -> {
            invocations.incrementAndGet();
            invocationsPerExecution.merge(ctx.executionId().value(), 1, Integer::sum);
            Thread.sleep(HANDLER_WORK);
        };

        try (ScenarioCluster cluster = new ScenarioCluster(dataSource, Clock.systemUTC())) {
            cluster.defineJob("churn", _ -> {
            }); // sem retries: o default do produto
            for (int i = 0; i < 3; i++) {
                cluster.addNode(settings(), List.of());
            }
            cluster.registerEverywhere("churn", handler);
            cluster.seedReady("churn", SEED, 20);
            cluster.startAll();

            ScenarioCluster.awaitUntil(Duration.ofSeconds(30),
                    () -> cluster.countLease() >= IN_FLIGHT_BEFORE_EXIT);
            Map<String, Integer> leasesByNodeAtExit = leasesByNode(cluster);
            cluster.nodes().get(2).engine().stop(Duration.ofSeconds(20));
            int heldByLeavingNode = leasesByNodeAtExit.getOrDefault(stoppedNodeId(cluster), 0);

            ScenarioCluster.Node joining = cluster.addNode(settings(), List.of());
            joining.handlers().register(JobKey.of("churn"), handler);
            joining.engine().start();

            boolean settled = ScenarioCluster.awaitUntil(OBSERVATION, cluster::isDrained);
            int failed = cluster.countTerminal("FAILED");

            System.out.printf("""

                    === Node churn — graceful stop with retries=0 (seed %d) ===
                    queue settled        : %s
                    handler invocations  : %d
                    held by leaving node : %d
                    terminal FAILED      : %d   <- work lost, bounded by what the node held
                    failure kinds        : %s
                    """, SEED, settled ? "yes" : "NO", invocations.get(), heldByLeavingNode, failed,
                    cluster.failureKinds());

            assertThat(settled).as("the queue must drain even when the exit costs work").isTrue();
            assertThat(failed)
                    .as("KNOWN EDGE: with retries=0 a reclaimed orphan has nowhere to be rescheduled and becomes a "
                            + "terminal FAILED. The damage must stay bounded by what the leaving node held (%d) — "
                            + "more than that means the loss is not the shutdown window", heldByLeavingNode)
                    .isLessThanOrEqualTo(heldByLeavingNode);
        }
    }

    /** Quantas leases cada nó segura AGORA — a foto que o bound de re-execução usa. */
    private static Map<String, Integer> leasesByNode(ScenarioCluster cluster) {
        return cluster.jdbc().query("SELECT node_id, count(*) AS held FROM mohs_lease GROUP BY node_id", rs -> {
            Map<String, Integer> held = new java.util.LinkedHashMap<>();
            while (rs.next()) {
                held.put(rs.getString("node_id"), rs.getInt("held"));
            }
            return held;
        });
    }

    /**
     * O nó que saiu é o único {@code STOPPED} — é assim que o cenário o
     * identifica sem alcançar o {@code nodeId} do Engine. Zero linhas não é
     * mistério: é a corrida que a ADR-0041 aceita de propósito (um tick que
     * leu o estado antes do {@code state.set(STOPPED)} commita o heartbeat
     * DEPOIS do final e sobrescreve a linha). Nomear a corrida aqui evita
     * que ela vire desconfiança do cenário inteiro.
     */
    private static String stoppedNodeId(ScenarioCluster cluster) {
        List<String> stopped = cluster.jdbc().queryForList(
                "SELECT node_id FROM mohs_nodes WHERE state = 'STOPPED'", String.class);
        assertThat(stopped)
                .as("expected exactly one STOPPED row; zero means the in-flight tick's heartbeat overwrote the final "
                        + "one (race accepted by ADR-0041/0051), and the re-execution bound has no baseline")
                .hasSize(1);
        return stopped.getFirst();
    }

    private static EngineSettings settings() {
        return new EngineSettings(Duration.ofMillis(25), Duration.ofMillis(200), 64, 32, 3,
                Duration.ofSeconds(30), Duration.ofSeconds(15), null, Duration.ofSeconds(60));
    }

    /** O que sobra quando algo se perdeu: quem era dono, e o histórico completo de algumas execuções mortas. */
    private static void dumpLostWork(ScenarioCluster cluster) {
        System.out.println("nodes at the end (the leaving one is the STOPPED row):");
        printRows(cluster, "SELECT node_id, state, last_heartbeat_at, expires_at FROM mohs_nodes");
        System.out.println("who owned the lost work:");
        printRows(cluster, """
                SELECT a.node_id, count(*) AS lost
                  FROM mohs_execution e JOIN mohs_attempt a ON a.execution_id = e.execution_id
                 WHERE e.state = 'FAILED'
                 GROUP BY a.node_id
                """);
        System.out.println("failed executions (up to 5, with every attempt):");
        printRows(cluster, """
                SELECT e.execution_id, e.state, e.scheduled_at, a.number, a.outcome, a.error_type, a.error,
                       a.started_at, a.finished_at, a.node_id
                  FROM mohs_execution e
                  JOIN mohs_attempt a ON a.execution_id = e.execution_id
                 WHERE e.state = 'FAILED'
                   AND e.execution_id IN (SELECT execution_id FROM mohs_execution WHERE state = 'FAILED' LIMIT 5)
                 ORDER BY e.execution_id, a.number
                """);
    }

    private static void printRows(ScenarioCluster cluster, String sql) {
        cluster.jdbc().queryForList(sql).forEach(row -> System.out.println("  " + row));
    }

    private static void report(boolean settled, int inFlightAtExit, long exitNanos, int invocations, int distinct,
            int succeeded, int failed, long reExecuted, Map<String, Integer> failureKinds) {
        System.out.printf("""

                === Node churn — one node leaves gracefully, one joins mid-drain (seed %d) ===
                queue settled        : %s
                in flight at exit    : %d
                graceful stop took   : %.2fs
                handler invocations  : %d over %d distinct executions
                re-executed          : %d
                terminal SUCCEEDED   : %d
                terminal FAILED      : %d
                failure kinds        : %s
                """, SEED, settled ? "yes" : "NO — still draining at the timeout", inFlightAtExit, exitNanos / 1e9,
                invocations, distinct, reExecuted, succeeded, failed, failureKinds);
    }
}
