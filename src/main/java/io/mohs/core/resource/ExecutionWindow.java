package io.mohs.core.resource;

import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Janela de exclusão de disparo: um job cujo horário cai em alguma exclusão
 * configurada não dispara. Predicados só existem em código — não há
 * equivalente em properties, ao contrário de {@link MohsRunner}/
 * {@link JobQueue}.
 *
 * <p>Os predicados desta primeira versão avaliam o {@link Instant} em UTC.
 * Se a exclusão precisar respeitar o zone do próprio job, isso é decisão do
 * motor ao consumir esta janela (M3), não deste contrato.
 *
 * <p><b>{@code equals()}/{@code hashCode()} gerados pelo record são, na
 * prática, baseados em identidade</b> (API-4): {@code exclusions} é uma
 * lista de {@link Predicate}, e cada chamada de
 * {@link Builder#excludeWeekends()}/{@link Builder#excludeDaily}/
 * {@link Builder#excludeDates}/{@link Builder#exclude} cria uma lambda
 * nova e distinta — duas janelas construídas com exatamente as mesmas
 * chamadas nunca são {@code equals()}. Semântica de valor de verdade
 * exigiria modelar as exclusões como dado selado
 * ({@code Weekends}/{@code Dates}/{@code DailyRange}/{@code Custom}) em
 * vez de predicados crus — mudança maior, fora de escopo enquanto nada
 * depender de igualdade/dedupe deste tipo.
 */
public record ExecutionWindow(String name, List<Predicate<Instant>> exclusions) {

    public ExecutionWindow {
        Objects.requireNonNull(name, "name");
        if (name.isBlank()) {
            throw new IllegalArgumentException("name must not be blank");
        }
        exclusions = List.copyOf(exclusions); // cópia defensiva (Effective Java, Item 50)
    }

    /** {@code true} se o instante cai em alguma exclusão configurada. */
    public boolean excludes(Instant instant) {
        return exclusions.stream().anyMatch(exclusion -> exclusion.test(instant));
    }

    public static Builder named(String name) {
        return new Builder(name);
    }

    public static final class Builder {
        private final String name;
        private final List<Predicate<Instant>> exclusions = new ArrayList<>();

        private Builder(String name) {
            this.name = name;
        }

        public Builder excludeWeekends() {
            exclusions.add(instant -> {
                DayOfWeek day = instant.atZone(ZoneOffset.UTC).getDayOfWeek();
                return day == DayOfWeek.SATURDAY || day == DayOfWeek.SUNDAY;
            });
            return this;
        }

        public Builder excludeDates(Collection<LocalDate> dates) {
            Set<LocalDate> copy = Set.copyOf(dates);
            exclusions.add(instant -> copy.contains(instant.atZone(ZoneOffset.UTC).toLocalDate()));
            return this;
        }

        /**
         * Exclui o intervalo diário meio-aberto {@code [from, to)}, em UTC.
         * Suporta cruzar a meia-noite (ex. {@code excludeDaily(22:00, 02:00)}
         * pra uma janela de manutenção noturna) — quando {@code from} é
         * depois de {@code to}, o intervalo é interpretado como
         * {@code [from, 24:00) ∪ [00:00, to)} em vez de virar um no-op
         * silencioso (TEST-9). {@code from} igual a {@code to} continua
         * vazio por definição, como qualquer intervalo meio-aberto
         * {@code [t, t)}.
         */
        public Builder excludeDaily(LocalTime from, LocalTime to) {
            Objects.requireNonNull(from, "from");
            Objects.requireNonNull(to, "to");
            if (from.equals(to)) {
                exclusions.add(instant -> false);
            } else if (from.isBefore(to)) {
                exclusions.add(instant -> {
                    LocalTime time = instant.atZone(ZoneOffset.UTC).toLocalTime();
                    return !time.isBefore(from) && time.isBefore(to);
                });
            } else {
                exclusions.add(instant -> {
                    LocalTime time = instant.atZone(ZoneOffset.UTC).toLocalTime();
                    return !time.isBefore(from) || time.isBefore(to);
                });
            }
            return this;
        }

        public Builder exclude(Predicate<Instant> predicate) {
            exclusions.add(Objects.requireNonNull(predicate, "predicate"));
            return this;
        }

        public ExecutionWindow build() {
            return new ExecutionWindow(name, exclusions);
        }
    }
}
