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

        /** Exclui o intervalo diário meio-aberto {@code [from, to)}, em UTC. */
        public Builder excludeDaily(LocalTime from, LocalTime to) {
            Objects.requireNonNull(from, "from");
            Objects.requireNonNull(to, "to");
            exclusions.add(instant -> {
                LocalTime time = instant.atZone(ZoneOffset.UTC).toLocalTime();
                return !time.isBefore(from) && time.isBefore(to);
            });
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
