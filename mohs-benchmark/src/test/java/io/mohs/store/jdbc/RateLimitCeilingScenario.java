package io.mohs.store.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;

import io.mohs.core.resource.RateLimit;
import io.mohs.engine.EngineSettings;

/**
 * S7 do §20.2 — limite a uma fração da demanda, cluster de 3 nós, e o job
 * SEM limite disputando o mesmo claim. Duas perguntas, e a segunda é a que
 * ninguém mediu: o teto segura sob concorrência (ADR-0042 item 4, CAS em
 * duas fases), e o job ilimitado paga ou não pelo vizinho limitado —
 * porque a rodada de claim que não fecha o CAS é desfeita INTEIRA
 * (item 4), e nessa rodada podem estar execuções de jobs sem limite
 * nenhum.
 *
 * <p>O critério de teto é o do token bucket, não o de janela fixa: a
 * capacidade é {@code max} e o refill é um token a cada
 * {@code window/max}, então o envelope legítimo da k-ésima entrega é
 * {@code t_k >= (k - max) × window/max}. Cobrar "nunca mais que max em
 * qualquer janela deslizante" seria cobrar um mecanismo que a ADR-0042
 * deliberadamente não escolheu (§ "Alternativa rejeitada: janela fixa").
 *
 * <p>Roda por nome: {@code ./mvnw -pl mohs-benchmark test
 * -Dtest=RateLimitCeilingScenario}.
 */
class RateLimitCeilingScenario {

    private static final String LIMIT_NAME = "smtp";
    private static final int MAX = 100;
    private static final Duration WINDOW = Duration.ofSeconds(10);
    private static final int NODES = 3;
    private static final int LIMITED_SEED = 1_200;
    private static final int UNLIMITED_SEED = 1_200;
    private static final Duration OBSERVATION = Duration.ofSeconds(45);
    /** O refill do bucket: um token a cada {@code window/max}. */
    private static final long TOKEN_PERIOD_NANOS = WINDOW.toNanos() / MAX;

    /** A primeira entrega que furou o envelope do bucket; {@link #NONE} quando nenhuma furou. */
    private record OverDelivery(int k, long aheadNanos) {
        static final OverDelivery NONE = new OverDelivery(-1, -1);
    }

    @Test
    void theCapHoldsAcrossNodesAndTheUnlimitedJobDoesNotPayForIt() {
        DataSource dataSource = PostgresTestSupport.freshSchema();
        Clock clock = Clock.systemUTC();
        List<Long> limitedStarts = Collections.synchronizedList(new ArrayList<>());
        List<Long> unlimitedStarts = Collections.synchronizedList(new ArrayList<>());

        try (ScenarioCluster cluster = new ScenarioCluster(dataSource, clock)) {
            cluster.rateLimits().upsert(new RateLimit(LIMIT_NAME, MAX, WINDOW));
            cluster.defineJob("limited", spec -> spec.rateLimit(LIMIT_NAME));
            cluster.defineJob("unlimited", _ -> {
            });

            for (int i = 0; i < NODES; i++) {
                cluster.addNode(settings(), List.of());
            }
            cluster.registerEverywhere("limited", (_, _) -> limitedStarts.add(System.nanoTime()));
            cluster.registerEverywhere("unlimited", (_, _) -> unlimitedStarts.add(System.nanoTime()));

            cluster.seedReady("limited", LIMITED_SEED, 20);
            cluster.seedReady("unlimited", UNLIMITED_SEED, 20);

            long startedAt = System.nanoTime();
            cluster.startAll();
            ScenarioCluster.awaitUntil(OBSERVATION, () -> unlimitedStarts.size() >= UNLIMITED_SEED
                    && limitedStarts.size() >= LIMITED_SEED);
            long observedNanos = System.nanoTime() - startedAt;

            // sorted(): a leitura do relógio e o add na lista não são atômicos,
            // então com ~192 handlers concorrentes a ORDEM DE INSERÇÃO não é a
            // ordem de tempo — e firstOverDelivery indexa k por posição contra
            // um envelope que cresce com k. Sem ordenar, um carimbo cedo numa
            // posição tardia inventa (ou esconde) uma violação.
            List<Long> limited = limitedStarts.stream().sorted().toList();
            List<Long> unlimited = unlimitedStarts.stream().sorted().toList();
            OverDelivery overDelivery = firstOverDelivery(limited, startedAt);
            long unlimitedDrainNanos = unlimited.size() < UNLIMITED_SEED ? -1
                    : unlimited.getLast() - startedAt;

            report(observedNanos, limited.size(), unlimited.size(), unlimitedDrainNanos, overDelivery);

            // duas provas, e a agregada é a que fecha a brecha do "smearing":
            // o token é cobrado no CLAIM e o carimbo é do HANDLER, então uma
            // rodada que cobrasse 300 tokens de uma vez poderia espalhar as 300
            // execuções ao longo da janela e caber no envelope por-k. O total
            // na janela não tem esse escape.
            assertThat(limited.size())
                    .as("aggregate over-delivery: %d deliveries in %.1fs, the bucket authorises at most %.0f",
                            limited.size(), observedNanos / 1e9, nominalDeliveries(observedNanos))
                    .isLessThanOrEqualTo((int) Math.ceil(nominalDeliveries(observedNanos)));
            assertThat(overDelivery)
                    .as("over-delivery: the %dth limited execution started %.3fs ahead of the token envelope",
                            overDelivery.k(), overDelivery.aheadNanos() / 1e9)
                    .isEqualTo(OverDelivery.NONE);
            assertThat(unlimited)
                    .as("the unlimited job must drain completely — it shares the claim rounds with a saturated limit")
                    .hasSize(UNLIMITED_SEED);
            assertThat(Duration.ofNanos(unlimitedDrainNanos))
                    .as("collateral damage: the unlimited job drained as slowly as the limited one")
                    .isLessThan(Duration.ofSeconds(20));

            assertThat(limited.size() / nominalDeliveries(observedNanos))
                    .as("under-delivery beyond the ADR-0042 item 5 burn (E4's kill line is 90%% of nominal)")
                    .isGreaterThan(0.90);
        }
    }

    /**
     * Varre as entregas do job limitado atrás da primeira que começou ANTES
     * do que o bucket permitiria: a k-ésima só é legítima a partir de
     * {@code (k − MAX) × TOKEN_PERIOD_NANOS} depois do arranque, porque as
     * MAX primeiras cabem no burst da capacidade cheia.
     */
    private static OverDelivery firstOverDelivery(List<Long> limitedStarts, long startedAt) {
        for (int k = MAX + 1; k <= limitedStarts.size(); k++) {
            long elapsed = limitedStarts.get(k - 1) - startedAt;
            long envelope = (long) (k - MAX) * TOKEN_PERIOD_NANOS;
            if (elapsed < envelope) {
                return new OverDelivery(k, envelope - elapsed);
            }
        }
        return OverDelivery.NONE;
    }

    /** Quantas entregas o bucket autoriza na janela observada: a capacidade inicial mais o refill. */
    private static double nominalDeliveries(long observedNanos) {
        return (double) observedNanos / TOKEN_PERIOD_NANOS + MAX;
    }

    private static EngineSettings settings() {
        return new EngineSettings(Duration.ofMillis(25), Duration.ofMillis(200), 64, 64, 3,
                Duration.ofSeconds(30), Duration.ofSeconds(15), null, Duration.ofSeconds(60));
    }

    private static void report(long observedNanos, int limited, int unlimited, long unlimitedDrainNanos,
            OverDelivery overDelivery) {
        System.out.printf("""

                === S7 — rate limit ceiling (%d nodes, max=%d per %s) ===
                observation window   : %.1fs
                limited delivered    : %d of %d seeded
                unlimited delivered  : %d of %d seeded
                unlimited full drain : %s
                nominal for window   : %.0f (capacity %d + refill)
                over-delivery        : %s
                """, NODES, MAX, WINDOW, observedNanos / 1e9, limited, LIMITED_SEED, unlimited, UNLIMITED_SEED,
                unlimitedDrainNanos < 0 ? "NOT REACHED" : "%.1fs".formatted(unlimitedDrainNanos / 1e9),
                nominalDeliveries(observedNanos), MAX,
                overDelivery.equals(OverDelivery.NONE) ? "none"
                        : "at k=%d, %.3fs early".formatted(overDelivery.k(), overDelivery.aheadNanos() / 1e9));
    }
}
