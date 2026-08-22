package io.mohs.engine;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.jspecify.annotations.Nullable;

import io.mohs.core.schedule.CronSpec;
import io.mohs.core.schedule.IntervalSpec;
import io.mohs.core.schedule.Misfire;
import io.mohs.core.schedule.OnDemandSpec;
import io.mohs.core.schedule.Schedule;

/**
 * Decide o que um trigger devido dispara (ADR-0035) — função pura sobre
 * ({@code schedule}, {@code misfire}, {@code next_fire_at}, {@code now}):
 * nenhum I/O, nenhuma leitura de relógio; quem chama fornece o "agora" do
 * {@code Clock} injetado, mesma regra de {@link NextFireCalculator}.
 *
 * <p>Ocorrência <b>perdida</b> é a mais velha que {@code misfireThreshold}
 * — devida dentro do threshold dispara atrasada em qualquer política
 * (atraso de até um poll-interval é operação normal, não falha). Só o
 * lote de perdidas responde à política: {@link Misfire#IGNORE} descarta,
 * {@link Misfire#FIRE_NOW} compensa com um único disparo imediato,
 * {@link Misfire#FIRE_ALL_MISSED} reproduz cada uma. O salto sobre
 * perdidas nunca as percorre uma a uma (cron recalcula do limiar;
 * fixed-rate salta por aritmética preservando a âncora da série).
 *
 * <p>Fixed-delay ({@code afterFinish}) é uma corrente de ocorrência
 * única: materializou, o próximo disparo é desconhecido até a execução
 * terminar ({@code nextFireAt} do plano sai {@code null} — a conclusão
 * rearma, ver {@code LeaseStore.CompletionResult#rearmNextFireAt}).
 */
public final class FiringPlanner {

    /**
     * Teto de ocorrências materializadas por job por ciclo — §3 do
     * documento mestre ("replay com cap 1.440 por ciclo, drenado, nunca
     * descartado"); aplicado a toda materialização, não só replay:
     * agenda patológica (intervalo de milissegundos) não transforma um
     * tick em inserção sem teto. Capado, {@code nextFireAt} do plano
     * fica devido e o excedente drena nos próximos ticks.
     */
    static final int MAX_OCCURRENCES_PER_CYCLE = 1440;

    private final NextFireCalculator calculator;
    private final Duration misfireThreshold;

    public FiringPlanner(NextFireCalculator calculator, Duration misfireThreshold) {
        this.calculator = Objects.requireNonNull(calculator, "calculator");
        this.misfireThreshold = Objects.requireNonNull(misfireThreshold, "misfireThreshold");
        if (!misfireThreshold.isPositive()) {
            throw new IllegalArgumentException("misfireThreshold must be positive, got " + misfireThreshold);
        }
    }

    /**
     * @param nextFireAt o {@code next_fire_at} observado — devido
     * ({@code <= now}) por contrato de quem chama
     * ({@code JobStore#findDueRecurring})
     * @throws IllegalArgumentException para {@link OnDemandSpec} (nunca é
     * devida — bug do chamador) ou cron irrealizável (nunca dispara)
     */
    public Plan plan(Schedule schedule, Misfire misfire, Instant nextFireAt, Instant now) {
        Objects.requireNonNull(schedule, "schedule");
        Objects.requireNonNull(misfire, "misfire");
        Objects.requireNonNull(nextFireAt, "nextFireAt");
        Objects.requireNonNull(now, "now");
        return switch (schedule) {
            case OnDemandSpec _ -> throw new IllegalArgumentException("on-demand schedules are never due");
            case IntervalSpec interval when interval.afterFinish() -> planAfterFinish(interval, misfire, nextFireAt, now);
            case IntervalSpec _, CronSpec _ -> planSeries(schedule, misfire, nextFireAt, now);
        };
    }

    private Plan planAfterFinish(IntervalSpec schedule, Misfire misfire, Instant nextFireAt, Instant now) {
        if (!missed(nextFireAt, now)) {
            return new Plan(List.of(nextFireAt), null, false);
        }
        return switch (misfire) {
            case IGNORE -> new Plan(List.of(), now.plus(schedule.interval()), true);
            // só uma ocorrência pode estar perdida numa corrente de fim-a-início — as duas políticas coincidem
            case FIRE_NOW, FIRE_ALL_MISSED -> new Plan(List.of(now), null, true);
        };
    }

    private Plan planSeries(Schedule schedule, Misfire misfire, Instant nextFireAt, Instant now) {
        boolean misfired = missed(nextFireAt, now);
        boolean compensate = misfired && misfire == Misfire.FIRE_NOW;
        // FIRE_ALL_MISSED reproduz da própria next_fire_at — nada a saltar
        Instant cursor = misfired && misfire != Misfire.FIRE_ALL_MISSED
                ? firstNotMissed(schedule, nextFireAt, now)
                : nextFireAt;
        // a compensação reserva a própria vaga no teto — "o cap vale para toda
        // materialização" (ADR-0035); a ocorrência deslocada continua devida e drena
        int walkCap = compensate ? MAX_OCCURRENCES_PER_CYCLE - 1 : MAX_OCCURRENCES_PER_CYCLE;
        List<Instant> occurrences = new ArrayList<>();
        while (!cursor.isAfter(now) && occurrences.size() < walkCap) {
            occurrences.add(cursor);
            cursor = calculator.nextFireAfter(schedule, cursor)
                    .orElseThrow(() -> new IllegalStateException("recurring schedule stopped producing occurrences: " + schedule));
        }
        if (compensate) {
            occurrences.add(now); // a compensação do lote perdido — a mais recente, fecha a ordem cronológica
        }
        return new Plan(List.copyOf(occurrences), cursor, misfired);
    }

    private boolean missed(Instant occurrence, Instant now) {
        return occurrence.isBefore(now.minus(misfireThreshold));
    }

    /**
     * Primeira ocorrência não perdida (≥ {@code now - threshold}), sem
     * percorrer as perdidas: cron recalcula direto do limiar (a série é
     * absoluta); fixed-rate salta por divisão inteira preservando a
     * âncora — a próxima ocorrência regular continua na série original,
     * nunca reancorada no instante do tick.
     */
    private Instant firstNotMissed(Schedule schedule, Instant seriesAnchor, Instant now) {
        Instant boundary = now.minus(misfireThreshold);
        return switch (schedule) {
            // minusNanos(1): nextFireAfter é estritamente-depois, e ocorrência exatamente no limiar não é perdida
            case CronSpec cron -> calculator.nextFireAfter(cron, boundary.minusNanos(1))
                    .orElseThrow(() -> new IllegalStateException("cron schedule stopped producing occurrences: " + cron));
            case IntervalSpec interval -> {
                Duration step = interval.interval();
                long steps = Duration.between(seriesAnchor, boundary).dividedBy(step);
                Instant candidate = seriesAnchor.plus(step.multipliedBy(steps));
                yield candidate.isBefore(boundary) ? candidate.plus(step) : candidate;
            }
            case OnDemandSpec _ -> throw new IllegalArgumentException("on-demand schedules are never due");
        };
    }

    /**
     * O veredito do planner para um trigger devido: os instantes a
     * materializar (ordem cronológica; {@code scheduled_at} de cada
     * ocorrência), o novo {@code next_fire_at} ({@code null} =
     * fixed-delay aguardando o fim; {@code <= now} = lote capado, ainda
     * devido) e se alguma ocorrência foi perdida ({@code misfired} — o
     * gatilho do WARN de quem chama).
     */
    public record Plan(List<Instant> occurrences, @Nullable Instant nextFireAt, boolean misfired) {

        public Plan {
            occurrences = List.copyOf(occurrences);
        }
    }
}
