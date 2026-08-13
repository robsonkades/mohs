package io.mohs.rest.job;

import java.time.ZoneId;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;

/** Forma de wire de {@link io.mohs.core.schedule.CronSpec}. */
public record CronView(ScheduleType type, String expression, ZoneId zone) implements ScheduleView {

    /**
     * Marca explicitamente o construtor canônico como o criador do Jackson
     * — sem isso, a ambiguidade com o construtor de conveniência de 2 args
     * abaixo faz o Jackson escolher o construtor errado na desserialização
     * (falha tentando popular {@code type}, que records não expõem via
     * setter).
     */
    @JsonCreator
    public CronView {
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(expression, "expression");
        Objects.requireNonNull(zone, "zone");
    }

    public CronView(String expression, ZoneId zone) {
        this(ScheduleType.CRON, expression, zone);
    }
}
