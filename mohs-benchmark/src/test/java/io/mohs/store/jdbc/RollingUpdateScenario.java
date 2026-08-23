package io.mohs.store.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;

import io.mohs.core.job.JobKey;
import io.mohs.engine.EngineSettings;
import io.mohs.engine.JobHandler;

/**
 * S9 do §20.2 — deploy que ADICIONA um job: durante a janela do rollout,
 * parte do cluster ainda não tem o handler. Dois cenários, porque são duas
 * perguntas com respostas diferentes, e misturá-las produziria um gate
 * permanentemente vermelho que todo mundo aprende a ignorar:
 *
 * <ol>
 *   <li>o rollout TERMINA dentro do orçamento de retry — o trabalho
 *       sobrevive? É a pergunta que decide se a release pode sair;</li>
 *   <li>um nó nunca aprende o handler — quanto custa? É o gap conhecido
 *       (decisão 2 do PLAN.md da Phase 6: handler-aware claiming ficou de
 *       fora), afirmado no valor que ele TEM hoje, para gritar quando
 *       piorar.</li>
 * </ol>
 *
 * <p>O custo do segundo é aritmética de sharding, não azar: {@code Shards.of}
 * é função do id, {@code ownedBy} parte 64 shards igualmente entre os 2 nós,
 * e o retry re-deriva o MESMO shard. Toda execução que cair nos 32 shards do
 * nó cego é reentregue a ele até o orçamento acabar — metade do backlog,
 * deterministicamente.
 *
 * <p>Roda por nome: {@code ./mvnw -pl mohs-benchmark test
 * -Dtest=RollingUpdateScenario}.
 */
class RollingUpdateScenario {

    private static final String JOB = "moving";
    private static final int SEED = 400;
    private static final int RETRIES = 3;
    private static final Duration OBSERVATION = Duration.ofSeconds(90);

    @Test
    void workSurvivesARolloutThatCompletesWithinTheRetryBudget() {
        DataSource dataSource = PostgresTestSupport.freshSchema();
        AtomicInteger executed = new AtomicInteger();
        JobHandler handler = (_, _) -> executed.incrementAndGet();

        try (ScenarioCluster cluster = new ScenarioCluster(dataSource, Clock.systemUTC())) {
            cluster.defineJob(JOB, spec -> spec.retries(RETRIES));
            ScenarioCluster.Node upgraded = cluster.addNode(settings(), List.of());
            ScenarioCluster.Node lagging = cluster.addNode(settings(), List.of());
            upgraded.handlers().register(JobKey.of(JOB), handler);

            cluster.seedReady(JOB, SEED, 20);
            cluster.startAll();

            // o rollout alcança o segundo nó: é o que um deploy real faz, e é
            // a diferença entre "janela de rollout" e "cluster heterogêneo
            // para sempre"
            ScenarioCluster.awaitUntil(Duration.ofSeconds(10), () -> executed.get() > 0);
            lagging.handlers().register(JobKey.of(JOB), handler);

            boolean settled = ScenarioCluster.awaitUntil(OBSERVATION, cluster::isDrained);
            int succeeded = cluster.countTerminal("SUCCEEDED");
            int failed = cluster.countTerminal("FAILED");

            report("rollout completes", settled, executed.get(), succeeded, failed, cluster.countAttempts(),
                    cluster.failureKinds());

            assertThat(settled)
                    .as("the queue must drain — a stalled queue is a different bug from a lost execution, "
                            + "and the assertions below cannot tell them apart")
                    .isTrue();
            assertThat(failed)
                    .as("a rollout that completes inside the retry budget must lose nothing")
                    .isZero();
            assertThat(succeeded).isEqualTo(SEED);
        }
    }

    /**
     * O gap conhecido, medido. Afirma o número ATUAL com o link para a
     * decisão que o criou — assim fica verde e ainda grita se piorar; uma
     * asserção de {@code failed == 0} aqui seria TODO disfarçado de teste,
     * porque cobra o handler-aware claiming que a Phase 6 não implementou.
     */
    @Test
    void aNodeThatNeverLearnsTheHandlerBurnsTheRetryBudgetOfItsShards() {
        DataSource dataSource = PostgresTestSupport.freshSchema();
        AtomicInteger executed = new AtomicInteger();

        try (ScenarioCluster cluster = new ScenarioCluster(dataSource, Clock.systemUTC())) {
            cluster.defineJob(JOB, spec -> spec.retries(RETRIES));
            ScenarioCluster.Node upgraded = cluster.addNode(settings(), List.of());
            cluster.addNode(settings(), List.of()); // cego para sempre
            upgraded.handlers().register(JobKey.of(JOB), (_, _) -> executed.incrementAndGet());

            cluster.seedReady(JOB, SEED, 20);
            cluster.startAll();

            boolean settled = ScenarioCluster.awaitUntil(OBSERVATION, cluster::isDrained);
            int succeeded = cluster.countTerminal("SUCCEEDED");
            int failed = cluster.countTerminal("FAILED");

            report("node stays blind", settled, executed.get(), succeeded, failed, cluster.countAttempts(),
                    cluster.failureKinds());

            assertThat(settled).as("the queue must drain even when half of it fails").isTrue();
            assertThat(succeeded + failed).as("every seeded execution must reach a terminal state").isEqualTo(SEED);
            assertThat(failed)
                    .as("KNOWN GAP (PLAN.md Phase 6, decision 2 — no handler-aware claiming): the blind node owns "
                            + "half of the 64 shards and burns the whole retry budget of every execution that lands "
                            + "there. A number far from half the seed means the shard assignment changed")
                    .isBetween(SEED / 2 - SEED / 20, SEED / 2 + SEED / 20);
            // pela MENSAGEM, não pelo tipo: o motor usa IllegalStateException
            // para handler ausente, shutdown e nó morto — só o texto separa
            // as três, e sem essa distinção a asserção não atribui nada
            assertThat(cluster.failureKinds().keySet())
                    .as("the loss must be attributable to the missing handler, not to a reclaim or a shutdown")
                    .anyMatch(kind -> kind.contains("no handler registered for job"));
        }
    }

    private static EngineSettings settings() {
        return new EngineSettings(Duration.ofMillis(25), Duration.ofMillis(200), 64, 32, 3,
                Duration.ofSeconds(30), Duration.ofSeconds(15), null, Duration.ofSeconds(60));
    }

    private static void report(String arm, boolean settled, int executed, int succeeded, int failed, int attempts,
            Map<String, Integer> failureKinds) {
        System.out.printf("""

                === S9 — rolling update, 2 nodes, 1 without the handler [%s] ===
                queue settled        : %s
                handler invocations  : %d of %d seeded
                terminal SUCCEEDED   : %d
                terminal FAILED      : %d
                attempts written     : %d (%.2f per execution)
                failure kinds        : %s
                """, arm, settled ? "yes" : "NO — still draining at the timeout", executed, SEED, succeeded, failed,
                attempts, attempts / (double) SEED, failureKinds);
    }
}
