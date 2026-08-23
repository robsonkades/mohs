package io.mohs.store.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;

import io.mohs.engine.EngineSettings;
import io.mohs.engine.JobHandler;

/**
 * Quanto custa um SIGTERM com o nó CHEIO — a pergunta operacional que o
 * {@link NodeChurnScenario} não responde, porque lá o interesse é o que se
 * perde, não quanto se espera. Aqui o nó sai com o dispatch saturado
 * ({@code dispatchConcurrency} execuções em voo, handler lento), e o que se
 * mede é o relógio: um orquestrador que espera
 * {@code terminationGracePeriodSeconds} precisa saber se o
 * {@code stop(grace)} termina em tempo de handler ou em tempo de grace.
 *
 * <p>O teto declarado é o contrato do drain (ADR-0007): drenar é esperar o
 * que está em voo TERMINAR, então o piso é a duração de um handler e o teto
 * é o {@code grace}. O que este cenário protege é o meio-termo — que a
 * espera não vire o grace inteiro por causa de trabalho que ninguém está
 * mais esperando de fato.
 *
 * <p>Roda por nome: {@code ./mvnw -pl mohs-benchmark test
 * -Dtest=ShutdownLatencyScenario}.
 */
class ShutdownLatencyScenario {

    private static final int SEED = 20_000;
    private static final int DISPATCH_CONCURRENCY = 256;
    /** Handler deliberadamente lento: é ele que mantém o nó CHEIO no instante do sinal. */
    private static final Duration HANDLER_WORK = Duration.ofMillis(250);
    private static final Duration GRACE = Duration.ofSeconds(30);

    @Test
    void aFullNodeShutsDownInHandlerTimeNotInGraceTime() {
        DataSource dataSource = PostgresTestSupport.freshSchema();
        AtomicInteger invocations = new AtomicInteger();
        AtomicInteger interrupted = new AtomicInteger();
        JobHandler handler = (_, _) -> {
            invocations.incrementAndGet();
            try {
                Thread.sleep(HANDLER_WORK);
            } catch (InterruptedException e) {
                interrupted.incrementAndGet();
                throw e;
            }
        };

        try (ScenarioCluster cluster = new ScenarioCluster(dataSource, Clock.systemUTC())) {
            cluster.defineJob("slow", _ -> {
            });
            for (int i = 0; i < 2; i++) {
                cluster.addNode(settings(), List.of());
            }
            cluster.registerEverywhere("slow", handler);
            cluster.seedReady("slow", SEED, 20);
            cluster.startAll();

            // o nó tem de estar CHEIO: esperar a posse do cluster chegar perto
            // do teto dos dois nós é o que faz o sinal cair no pior instante
            ScenarioCluster.awaitUntil(Duration.ofSeconds(60),
                    () -> cluster.countLease() >= DISPATCH_CONCURRENCY);
            int inFlightAtSignal = cluster.countLease();

            long signalAt = System.nanoTime();
            cluster.nodes().getFirst().engine().stop(GRACE);
            Duration stopTook = Duration.ofNanos(System.nanoTime() - signalAt);

            System.out.printf("""

                    === Shutdown latency — one node of two, dispatch saturated ===
                    in flight at signal  : %d (cluster), dispatch cap %d per node
                    stop(grace=%s) took  : %.2fs
                    handler duration     : %s  <- the floor a graceful drain cannot beat
                    handlers interrupted : %d  <- non-zero means the grace was exhausted
                    invocations          : %d
                    """, inFlightAtSignal, DISPATCH_CONCURRENCY, GRACE, stopTook.toNanos() / 1e9, HANDLER_WORK,
                    interrupted.get(), invocations.get());

            assertThat(stopTook)
                    .as("a graceful stop must finish in handler time, not in grace time — %s means it waited for "
                            + "something nobody was waiting for", stopTook)
                    .isLessThan(GRACE.dividedBy(3));
            assertThat(interrupted.get())
                    .as("no handler may be interrupted: the grace was far longer than the work in flight")
                    .isZero();
        }
    }

    private static EngineSettings settings() {
        return new EngineSettings(Duration.ofMillis(25), Duration.ofMillis(200), 256, DISPATCH_CONCURRENCY, 3,
                Duration.ofSeconds(30), Duration.ofSeconds(15), null, Duration.ofSeconds(60));
    }
}
