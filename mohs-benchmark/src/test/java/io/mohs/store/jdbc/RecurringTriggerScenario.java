package io.mohs.store.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;

import io.mohs.core.job.JobKey;
import io.mohs.engine.EngineSettings;

/**
 * O dia a dia do produto: um job recorrente, um cluster de 3 nós, e a
 * pergunta do CLAUDE.md — "e se dois nós dispararem o mesmo trigger?".
 * A materialização é CAS sobre a definição (§5.2, leader-free): a ocorrência
 * é materializada por quem ganhar o CAS, e os outros dois seguem em frente.
 * Duplicar aqui não é entregar duas vezes a mesma execução (isso o claim
 * cobre) — é criar DUAS execuções para a MESMA ocorrência, que nenhum
 * consumidor idempotente detecta, porque são ids diferentes.
 *
 * <p>Mede também o que ninguém mediu: a PONTUALIDADE. O produto promete um
 * piso de latência de dispatch (ADR-0056) e a régua honesta é
 * {@code started_at − scheduled_at} sobre disparos reais, não sobre um
 * enqueue sintético.
 *
 * <p>E fecha com o pause do dia a dia (ADR-0037): pausado, nada de novo é
 * materializado; retomado, o {@code FiringPlanner} materializa TODAS as
 * ocorrências perdidas de uma vez. Não é misfire: pela ADR-0035, ocorrência
 * devida dentro do {@code misfireThreshold} (60s aqui) dispara atrasada em
 * QUALQUER política, {@code IGNORE} inclusive — o {@code Misfire} do job só
 * decide o que fazer com o que é mais velho que o threshold. A consequência
 * operacional é uma rajada no resume proporcional à pausa, e o cenário
 * afirma isso em vez de fingir que o resume só retoma a cadência.
 *
 * <p>Escopo desta prova de unicidade: {@code duplicateOccurrences} agrupa
 * por {@code scheduled_at}, o que só enxerga dupla materialização no caminho
 * SEM misfire — na compensação {@code FIRE_NOW} o planner carimba
 * {@code now} e dois nós teriam instantes diferentes. Este cenário nunca
 * cruza o threshold, então a prova vale para o caminho pontual.
 *
 * <p>Roda por nome: {@code ./mvnw -pl mohs-benchmark test
 * -Dtest=RecurringTriggerScenario}.
 */
class RecurringTriggerScenario {

    private static final String JOB = "ticker";
    private static final Duration EVERY = Duration.ofSeconds(1);
    private static final Duration RUN = Duration.ofSeconds(20);
    private static final Duration PAUSED = Duration.ofSeconds(6);
    private static final int NODES = 3;
    /** Quantas ocorrências se espera observar depois do resume, além da janela pausada reproduzida. */
    private static final int RESUME_OBSERVATION_TICKS = 4;

    @Test
    void oneOccurrenceFiresOnceAcrossTheClusterAndPauseHolds() throws Exception {
        DataSource dataSource = PostgresTestSupport.freshSchema();
        Clock clock = Clock.systemUTC();

        try (ScenarioCluster cluster = new ScenarioCluster(dataSource, clock)) {
            cluster.defineRecurring(JOB, EVERY);
            for (int i = 0; i < NODES; i++) {
                cluster.addNode(settings(), List.of());
            }
            cluster.registerEverywhere(JOB, (_, _) -> {
            });
            cluster.startAll();

            Thread.sleep(RUN.toMillis());

            int firedWhileRunning = cluster.countExecutionsOf(JOB);
            List<Map<String, Object>> duplicates = duplicateOccurrences(cluster);
            Map<String, Object> lateness = lateness(cluster);

            cluster.jobs().pause(JobKey.of(JOB));
            // uma ocorrência de folga: o pause vale para a MATERIALIZAÇÃO, e o
            // que já foi materializado antes dele ainda dispara
            Thread.sleep(EVERY.toMillis() * 2);
            int atPause = cluster.countExecutionsOf(JOB);
            Thread.sleep(PAUSED.toMillis());
            int afterPause = cluster.countExecutionsOf(JOB);

            cluster.jobs().resume(JobKey.of(JOB));
            Thread.sleep(EVERY.toMillis() * RESUME_OBSERVATION_TICKS);
            int afterResume = cluster.countExecutionsOf(JOB);

            long expectedWhileRunning = RUN.dividedBy(EVERY);
            report(firedWhileRunning, expectedWhileRunning, duplicates, lateness, atPause, afterPause, afterResume);

            assertThat(duplicates)
                    .as("two nodes materialised the same occurrence — the CAS advance of §5.2 did not hold")
                    .isEmpty();
            assertThat(firedWhileRunning)
                    .as("a %s job over %s should fire about %d times", EVERY, RUN, expectedWhileRunning)
                    .isBetween((int) expectedWhileRunning - 2, (int) expectedWhileRunning + 2);
            assertThat(afterPause - atPause).as("a paused job must not materialise new occurrences").isZero();
            // o resume REPRODUZ a janela pausada (dentro do misfireThreshold,
            // ADR-0035): a rajada esperada é a pausa inteira mais as
            // ocorrências do intervalo de observação, e afirmar só "> 0"
            // deixaria passar um resume que perdesse a janela em silêncio
            long expectedBurst = PAUSED.plus(EVERY.multipliedBy(RESUME_OBSERVATION_TICKS)).dividedBy(EVERY);
            assertThat(afterResume - afterPause)
                    .as("resume must bring the job back AND replay the paused window (%d occurrences within the "
                            + "misfire threshold)", expectedBurst)
                    .isGreaterThanOrEqualTo((int) expectedBurst - 2);
        }
    }

    /** Duas execuções para o MESMO {@code scheduled_at} é a assinatura exata de dupla materialização. */
    private static List<Map<String, Object>> duplicateOccurrences(ScenarioCluster cluster) {
        return cluster.jdbc().queryForList("""
                SELECT scheduled_at, count(*) AS total
                  FROM mohs_execution
                 WHERE job_key = ?
                 GROUP BY scheduled_at
                HAVING count(*) > 1
                """, JOB);
    }

    /** {@code started_at − scheduled_at} em milissegundos: o atraso real do disparo recorrente. */
    private static Map<String, Object> lateness(ScenarioCluster cluster) {
        return cluster.jdbc().queryForMap("""
                SELECT round(avg(delay_ms))                                            AS avg_ms,
                       round(percentile_cont(0.5) WITHIN GROUP (ORDER BY delay_ms))    AS p50_ms,
                       round(percentile_cont(0.95) WITHIN GROUP (ORDER BY delay_ms))   AS p95_ms,
                       round(max(delay_ms))                                            AS max_ms
                  FROM (SELECT EXTRACT(EPOCH FROM (a.started_at - e.scheduled_at)) * 1000 AS delay_ms
                          FROM mohs_execution e
                          JOIN mohs_attempt a ON a.execution_id = e.execution_id AND a.number = 1
                         WHERE e.job_key = ?) delays
                """, JOB);
    }

    private static EngineSettings settings() {
        return new EngineSettings(Duration.ofMillis(25), Duration.ofMillis(200), 32, 16, 1,
                Duration.ofSeconds(30), Duration.ofSeconds(15), null, Duration.ofSeconds(60));
    }

    private static void report(int fired, long expected, List<Map<String, Object>> duplicates,
            Map<String, Object> lateness, int atPause, int afterPause, int afterResume) {
        System.out.printf("""

                === Recurring trigger — %s job, %d nodes ===
                fired in %s          : %d (expected ~%d)
                duplicate occurrences: %d %s
                dispatch lateness    : avg %s ms · p50 %s ms · p95 %s ms · max %s ms
                paused at            : %d executions
                after %s paused      : %d (delta %d — must be 0)
                after resume         : %d (delta %d — must be > 0)
                """, EVERY, NODES, RUN, fired, expected, duplicates.size(), duplicates.isEmpty() ? "" : duplicates,
                lateness.get("avg_ms"), lateness.get("p50_ms"), lateness.get("p95_ms"), lateness.get("max_ms"),
                atPause, PAUSED, afterPause, afterPause - atPause, afterResume, afterResume - afterPause);
    }
}
