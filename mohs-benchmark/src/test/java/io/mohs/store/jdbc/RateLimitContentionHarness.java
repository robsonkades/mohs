package io.mohs.store.jdbc;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import tools.jackson.databind.json.JsonMapper;

import io.mohs.core.definition.JobDefinition;
import io.mohs.core.resource.RateLimit;
import io.mohs.engine.ExecutionWindowRegistry;
import io.mohs.store.jdbc.dialect.PostgresJdbcDialect;

/**
 * Mede o custo de contenção do balde de rate limit (ADR-0042) no caminho
 * REAL de claim, com N nós disputando o mesmo limite quente — não um proxy
 * SQL. Existe porque a revisão de duas fases (cobrança no FIM da transação,
 * via CAS) foi decidida sobre números de proxy: aqui a comparação é com o
 * motor.
 *
 * <p>Dois cenários, porque medem coisas diferentes:
 * <ul>
 * <li><b>Coordenação</b> — balde grande demais para throttlar. Isola o custo
 * de coordenar, sem o efeito do limite. Métrica: vazão até drenar o backlog.
 * <li><b>Throttlado</b> — balde pequeno, refill escasso, todo nó disputando
 * poucos tokens. É AQUI que a colisão de CAS mora, e portanto onde o
 * descarte de rodada (o preço que a revisão de duas fases cobra) aparece.
 * Medir só o primeiro cenário seria medir o caso favorável.
 * </ul>
 *
 * <p>Fora da suíte unitária de propósito (CLAUDE.md: "benchmarks vivem
 * apart") — o nome não casa o include default do Surefire. Sob demanda:
 * {@code mvn test -Dtest=RateLimitContentionHarness}. Precisa do Docker
 * (Testcontainers sobe o Postgres).
 */
class RateLimitContentionHarness {

    private static final Duration LEASE_TTL = Duration.ofMinutes(5);
    private static final int BATCH_SIZE = 1000;
    private static final int BACKLOG = 120_000;
    private static final int[] NODE_COUNTS = {1, 2, 4, 8};

    /** Grande o bastante pra nunca ser o gargalo: mede coordenação, não throttling. */
    private static final RateLimit UNTHROTTLED = new RateLimit("hot", 100_000_000, Duration.ofHours(1));

    /**
     * Balde que esvazia nas primeiras rodadas e depois só pinga (5.000 por
     * minuto ≈ 83/s): a partir daí todo nó lê um saldo pequeno e disputa os
     * mesmos poucos tokens — a condição de colisão máxima do CAS.
     */
    private static final RateLimit THROTTLED = new RateLimit("hot", 5_000, Duration.ofMinutes(1));

    private static final Duration THROTTLED_RUN = Duration.ofSeconds(6);

    /** O cenário A não tem janela de tempo: cada nó para quando o backlog seca (três vazias seguidas). */
    private static final Duration UNTIL_BACKLOG_DRIES = null;

    @Test
    void run() throws Exception {
        List<String> coordination = new ArrayList<>();
        for (int nodes : NODE_COUNTS) {
            coordination.add(drainBacklog(nodes));
        }
        List<String> throttled = new ArrayList<>();
        for (int nodes : NODE_COUNTS) {
            throttled.add(runThrottled(nodes));
        }

        System.out.printf("%n=== A) Custo de coordenação — balde nunca throttla (batch=%d, backlog=%d) ===%n",
                BATCH_SIZE, BACKLOG);
        System.out.printf("%-6s %12s %10s %10s %10s%n", "nós", "exec/s", "rodadas", "vazias", "segundos");
        coordination.forEach(System.out::println);

        System.out.printf("%n=== B) Regime THROTTLADO — balde de %d/%s, %ds por nível ===%n",
                THROTTLED.max(), THROTTLED.window(), THROTTLED_RUN.toSeconds());
        System.out.printf("%-6s %12s %10s %10s %10s%n", "nós", "exec/s", "rodadas", "vazias", "vazias%");
        throttled.forEach(System.out::println);
    }

    private String drainBacklog(int nodes) throws Exception {
        Run result = race(seed(UNTHROTTLED), nodes, UNTIL_BACKLOG_DRIES);
        double seconds = result.elapsed().toNanos() / 1_000_000_000.0;
        return String.format("%-6d %12.0f %10d %10d %10.2f",
                nodes, result.claimed() / seconds, result.rounds(), result.emptyRounds(), seconds);
    }

    private String runThrottled(int nodes) throws Exception {
        Run result = race(seed(THROTTLED), nodes, THROTTLED_RUN);
        double seconds = result.elapsed().toNanos() / 1_000_000_000.0;
        double discardPct = result.rounds() + result.emptyRounds() == 0
                ? 0
                : 100.0 * result.emptyRounds() / (result.rounds() + result.emptyRounds());
        return String.format("%-6d %12.0f %10d %10d %9.1f%%",
                nodes, result.claimed() / seconds, result.rounds(), result.emptyRounds(), discardPct);
    }

    /**
     * Os nós largam juntos (barrier) e cada um roda a MESMA rodada de claim
     * até seu critério de parada: {@link #UNTIL_BACKLOG_DRIES} para na
     * primeira rodada vazia, uma duração roda a janela inteira — no cenário
     * throttlado a rodada vazia é o caso normal, não o fim do trabalho.
     */
    private Run race(Fixture fixture, int nodes, Duration limit) throws Exception {
        boolean untilBacklogDries = limit == UNTIL_BACKLOG_DRIES;
        AtomicLong rounds = new AtomicLong();
        AtomicLong claimed = new AtomicLong();
        AtomicLong emptyRounds = new AtomicLong();
        CyclicBarrier start = new CyclicBarrier(nodes);
        List<Callable<Void>> workers = new ArrayList<>(nodes);
        for (int node = 0; node < nodes; node++) {
            JdbcClaimer claimer = fixture.newClaimer();
            workers.add(() -> {
                start.await(1, TimeUnit.MINUTES);
                long deadline = untilBacklogDries ? Long.MAX_VALUE : System.nanoTime() + limit.toNanos();
                int consecutiveEmpty = 0;
                while (System.nanoTime() < deadline) {
                    int taken = claimer.claim("node", BATCH_SIZE).size();
                    rounds.incrementAndGet();
                    if (taken > 0) {
                        consecutiveEmpty = 0;
                        claimed.addAndGet(taken);
                        continue;
                    }
                    // Rodada VAZIA, não exceção: desde o gate de ciclo 2 a
                    // rodada desfeita pelo balde volta como lista vazia em vez
                    // de propagar, então contar pelo retorno é obrigatório —
                    // o `catch` de antes virou instrumento morto, e
                    // instrumento que mede zero é indistinguível de
                    // instrumento quebrado.
                    //
                    // No cenário A a vazia é o FIM DO BACKLOG (e ainda assim
                    // exige confirmação: uma vazia isolada pode ser
                    // contenção); no B ela é o teto de descarte, não o
                    // descarte exato — conflaciona "rodada desfeita pelo CAS"
                    // com "balde vazio, ninguém admitido, nada a cobrar". A
                    // separação exata exigiria instrumentar a produção só
                    // para medir, e o teto já responde a pergunta que
                    // importa: quanto trabalho se perde sob throttling.
                    if (untilBacklogDries) {
                        if (++consecutiveEmpty == 3) {
                            return null;
                        }
                        continue;
                    }
                    emptyRounds.incrementAndGet();
                }
                return null;
            });
        }

        ExecutorService executor = Executors.newFixedThreadPool(nodes);
        long startedAt = System.nanoTime();
        for (Future<Void> worker : executor.invokeAll(workers)) {
            worker.get(10, TimeUnit.MINUTES);
        }
        Duration elapsed = Duration.ofNanos(System.nanoTime() - startedAt);
        executor.shutdown();
        return new Run(rounds.get(), claimed.get(), emptyRounds.get(), elapsed);
    }

    /** Um job só, um limite só: o pior caso de contenção — todo nó disputa a MESMA linha de balde em toda rodada. */
    private Fixture seed(RateLimit rateLimit) {
        DataSource dataSource = PostgresTestSupport.freshSchema();
        Clock clock = Clock.systemUTC();
        JdbcExecutionStore executionStore = new JdbcExecutionStore(dataSource, clock, JsonMapper.builder().build(), new PostgresJdbcDialect());
        JdbcJobStore jobStore = new JdbcJobStore(dataSource, clock);
        JdbcRateLimitStore rateLimitStore = new JdbcRateLimitStore(dataSource, clock);
        rateLimitStore.upsert(rateLimit);
        jobStore.upsert(JobDefinition.of("hot-job", Handler.class, spec -> spec.onDemand().rateLimit(rateLimit.name())));
        Instant now = Instant.now().minusSeconds(60);
        new JdbcTemplate(dataSource).update("""
                INSERT INTO mohs_executions (id, job_key, state, scheduled_at, actor, priority, payload, payload_type, created_at)
                SELECT lpad(n::text, 26, '0'), 'hot-job', 'ENQUEUED', ?, 'harness', 20, '{}', 'java.lang.Object', ?
                FROM generate_series(1, ?) n
                """, JdbcTimestamps.toUtcLocalDateTime(now), JdbcTimestamps.toUtcLocalDateTime(now), BACKLOG);
        return new Fixture(dataSource, clock, executionStore, jobStore, rateLimitStore);
    }

    private record Run(long rounds, long claimed, long emptyRounds, Duration elapsed) {
    }

    /** Handler só para o job existir: a harness mede o claim, nunca chega a executar nada. */
    record Handler() {
    }

    private record Fixture(DataSource dataSource, Clock clock, JdbcExecutionStore executionStore,
            JdbcJobStore jobStore, JdbcRateLimitStore rateLimitStore) {

        JdbcClaimer newClaimer() {
            return new JdbcClaimer(dataSource, new PostgresJdbcDialect(), clock, executionStore, jobStore,
                    LEASE_TTL, new ExecutionWindowRegistry(List.of()), rateLimitStore);
        }
    }
}
