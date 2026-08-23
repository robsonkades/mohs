package io.mohs.store.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;

import io.mohs.core.Batch;
import io.mohs.core.event.BatchCompleted;
import io.mohs.core.event.ExecutionListener;
import io.mohs.core.event.Failed;
import io.mohs.core.event.Succeeded;
import io.mohs.core.job.JobRef;
import io.mohs.engine.BatchCompletionCallbacks;
import io.mohs.engine.EngineSettings;

/**
 * S10 do §20.2 — um lote grande criado pela fachada pública e drenado por
 * um cluster de 3 nós. O critério é o contador da ADR-0043: o
 * {@code BatchCompleted} nasce do {@code UPDATE ... RETURNING} que fecha o
 * lote, então DOIS nós concluindo o penúltimo e o último membro ao mesmo
 * tempo têm de produzir UM evento, não dois — e nenhum se a corrida
 * perdesse o incremento.
 *
 * <p>Membros que FALHAM contam igual (o lote fecha por total, não por
 * sucesso): metade dos membros lança de propósito, o que também exercita o
 * caminho de {@code Failed} fechando lote. O job declara
 * {@code retries(1)} — não é detalhe de bancada, é o que torna a asserção
 * do contador FALSIFICÁVEL: cada membro que falha é invocado DUAS vezes e
 * tem de contar UMA, e é isso que separa "contou a falha terminal" de
 * "contou cada tentativa", que fecharia o lote cedo. Com o default do
 * produto ({@code retries = 0}) os dois comportamentos produziriam o mesmo
 * número e a asserção não provaria nada.
 *
 * <p>Roda por nome: {@code ./mvnw -pl mohs-benchmark test
 * -Dtest=BatchCompletionScenario}.
 */
class BatchCompletionScenario {

    private static final int MEMBERS = 20_000;
    /**
     * Um retry além da primeira tentativa: é o que torna FALSIFICÁVEL a
     * asserção sobre o contador. Com o default do produto (retries=0) cada
     * membro é invocado UMA vez, e aí "conta a falha terminal" e "conta cada
     * attempt" produzem o mesmo número — um contador por-attempt passaria
     * verde. Com um retry, o membro que falha é invocado DUAS vezes e tem
     * de contar UMA.
     */
    private static final int RETRIES = 1;
    private static final int NODES = 3;
    private static final Duration DRAIN_TIMEOUT = Duration.ofMinutes(3);
    /**
     * Janela extra depois do fechamento: um SEGUNDO evento chegaria aqui, e
     * afirmar "exatamente um" sem esperar por ele seria afirmar sobre uma
     * corrida que ainda não terminou.
     */
    private static final Duration SECOND_EVENT_WINDOW = Duration.ofSeconds(5);

    @Test
    void aLargeBatchClosesExactlyOnceUnderConcurrentCompletion() {
        DataSource dataSource = PostgresTestSupport.freshSchema();
        Clock clock = Clock.systemUTC();
        List<BatchCompleted> completions = Collections.synchronizedList(new ArrayList<>());
        AtomicInteger invocations = new AtomicInteger();
        AtomicInteger failures = new AtomicInteger();
        AtomicInteger terminalEvents = new AtomicInteger();
        ExecutionListener collector = event -> {
            switch (event) {
                case BatchCompleted completed -> completions.add(completed);
                case Succeeded _, Failed _ -> terminalEvents.incrementAndGet();
                default -> {
                }
            }
        };
        BatchCompletionCallbacks callbacks = new BatchCompletionCallbacks();

        try (ScenarioCluster cluster = new ScenarioCluster(dataSource, clock)) {
            cluster.defineJob("member", spec -> spec.retries(RETRIES));
            for (int i = 0; i < NODES; i++) {
                cluster.addNode(settings(), List.of(collector, callbacks));
            }
            cluster.registerEverywhere("member", (payload, _) -> {
                invocations.incrementAndGet();
                if (Integer.parseInt(payload.toString()) % 2 == 0) {
                    failures.incrementAndGet();
                    throw new IllegalStateException("deliberate member failure");
                }
            });
            cluster.startAll();

            JobRef<String> member = JobRef.of("member", String.class);
            long createdAt = System.nanoTime();
            Batch batch = cluster.facadeFor(cluster.nodes().getFirst(), callbacks)
                    .batch("release-validation", builder -> {
                        for (int i = 0; i < MEMBERS; i++) {
                            builder.add(member, String.valueOf(i));
                        }
                    });
            long createNanos = System.nanoTime() - createdAt;

            boolean closed = ScenarioCluster.awaitUntil(DRAIN_TIMEOUT, () -> !completions.isEmpty());
            long drainNanos = System.nanoTime() - createdAt;
            ScenarioCluster.awaitUntil(SECOND_EVENT_WINDOW, () -> completions.size() > 1);

            report(createNanos, drainNanos, closed, completions, invocations.get(), failures.get(),
                    cluster.countReady(), cluster.countLease(), terminalEvents.get());

            // A evidência PRIMÁRIA vem da tabela, não do evento: mohs_batches
            // é a fonte de verdade da ADR-0043 e não perde nada. O canal de
            // eventos é best-effort por contrato, então ele é verificado
            // (abaixo) mas nunca é a única testemunha.
            Map<String, Object> counters = cluster.jdbc().queryForMap(
                    "SELECT total, succeeded, failed FROM mohs_batches WHERE id = ?", batch.batchId());
            assertThat(counters.get("total")).isEqualTo(MEMBERS);
            assertThat(counters.get("failed"))
                    .as("the batch counter must count the TERMINAL failure once, not each attempt — with retries=%d "
                            + "each deliberate failure is invoked twice", RETRIES)
                    .isEqualTo(MEMBERS / 2);
            assertThat(counters.get("succeeded")).isEqualTo(MEMBERS / 2);

            // com um retry, cada membro par roda duas vezes: a igualdade
            // ESTRITA é o que separa "o contador é terminal" de "o contador é
            // por attempt", que a desigualdade frouxa deixaria passar
            assertThat(invocations.get())
                    .as("every deliberate failure must be invoked twice with retries=%d — otherwise the counter "
                            + "assertion above cannot tell terminal counting from per-attempt counting", RETRIES)
                    .isEqualTo(MEMBERS + MEMBERS / 2);

            // e só agora o canal: se todo evento terminal chegou, ele não
            // descartou nada, e a contagem de BatchCompleted significa algo
            assertThat(terminalEvents.get())
                    .as("the event channel dropped events — the BatchCompleted assertion below would be vacuous")
                    .isEqualTo(MEMBERS);
            assertThat(completions).as("BatchCompleted must fire exactly once for batch %s", batch.batchId())
                    .hasSize(1);
            BatchCompleted completed = completions.getFirst();
            assertThat(completed.total()).isEqualTo(MEMBERS);
            assertThat(completed.succeeded() + completed.failed())
                    .as("every member must be counted exactly once in the closing snapshot")
                    .isEqualTo(MEMBERS);
            assertThat(cluster.countReady() + cluster.countLease())
                    .as("queue and lease must be empty once the batch closed")
                    .isZero();
        }
    }

    private static EngineSettings settings() {
        return new EngineSettings(Duration.ofMillis(25), Duration.ofMillis(200), 256, 128, 3,
                Duration.ofSeconds(30), Duration.ofSeconds(15), null, Duration.ofSeconds(60));
    }

    private static void report(long createNanos, long drainNanos, boolean closed, List<BatchCompleted> completions,
            int invocations, int failures, int ready, int lease, int terminalEvents) {
        System.out.printf("""

                === S10 — batch completion (%d members, %d nodes) ===
                batch creation       : %.1fs (%.0f members/s)
                drain to completion  : %s
                BatchCompleted count : %d %s
                handler invocations  : %d (%d deliberate failures)
                left in queue/lease  : %d / %d
                terminal events seen : %d of %d (a shortfall means the event channel dropped)
                """, MEMBERS, NODES, createNanos / 1e9, MEMBERS / (createNanos / 1e9),
                closed ? "%.1fs".formatted(drainNanos / 1e9) : "NOT REACHED", completions.size(),
                completions.isEmpty() ? "" : completions.getFirst().toString(), invocations, failures, ready, lease, terminalEvents, MEMBERS);
    }
}
