package io.mohs.engine;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import io.mohs.core.schedule.CronSpec;
import io.mohs.core.schedule.IntervalSpec;
import io.mohs.core.schedule.OnDemandSpec;
import io.mohs.core.schedule.Schedule;
import io.mohs.cron.CronExpression;

/**
 * Calcula o próximo disparo de uma {@link Schedule}. Nunca lê o relógio —
 * quem chama fornece {@code reference} (o "agora" do {@code Clock}
 * injetado do motor); esta regra é o que torna testes de agenda
 * determinísticos, sem {@code Thread.sleep} (§CLAUDE.md "O que NÃO fazer").
 *
 * <p>Para {@link CronSpec}, delega ao parser vendorizado em
 * {@link io.mohs.cron}, com o resultado do parse cacheado por expressão
 * (§3 do documento mestre: "expressões cacheadas") — parsing não é caro,
 * mas evitar repeti-lo a cada poll do claim loop é mechanical sympathy
 * barata. Para {@link IntervalSpec}, soma o intervalo a {@code reference}
 * — fixed-rate vs. fixed-delay não é decisão desta classe: quem chama
 * escolhe o que passar como {@code reference} (horário agendado vs. fim da
 * execução anterior). {@link OnDemandSpec} nunca dispara sozinho.
 */
public final class NextFireCalculator {

    private final Map<String, CronExpression> CRON_CACHE = new ConcurrentHashMap<>();

    /**
     * Próximo disparo estritamente após {@code reference}. Vazio só para
     * {@link OnDemandSpec}.
     *
     * @throws IllegalArgumentException se a expressão cron nunca dispara
     * (ex.: 30 de fevereiro) — sintaticamente válida, mas irrealizável
     */
    public Optional<Instant> nextFireAfter(Schedule schedule, Instant reference) {
        return switch (schedule) {
            case CronSpec cron -> Optional.of(nextCronFire(cron, reference));
            case IntervalSpec interval -> Optional.of(reference.plus(interval.interval()));
            case OnDemandSpec onDemand -> Optional.empty();
        };
    }

    private Instant nextCronFire(CronSpec cron, Instant reference) {
        CronExpression parsed = CRON_CACHE.computeIfAbsent(cron.expression(), CronExpression::parse);
        ZonedDateTime seed = reference.atZone(cron.zone());
        ZonedDateTime next = parsed.next(seed);
        if (next == null) {
            throw new IllegalArgumentException("Cron expression never fires within the search bound: " + cron.expression());
        }
        return next.toInstant();
    }
}
