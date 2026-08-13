package io.mohs.rest.job;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import io.mohs.core.schedule.CronSpec;
import io.mohs.core.schedule.IntervalSpec;
import io.mohs.core.schedule.OnDemandSpec;
import io.mohs.core.schedule.Schedule;

/**
 * Forma de wire de {@link Schedule} — selado 1:1 com o domínio (mesmo
 * espírito: switch exaustivo em {@link #from(Schedule)}, quarto tipo é
 * erro de compilação até tratado). {@code type} é discriminador explícito
 * no JSON, não inferido pelo nome da classe (portável entre linguagens de
 * cliente).
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "type", visible = true)
@JsonSubTypes({
        @JsonSubTypes.Type(value = CronView.class, name = "CRON"),
        @JsonSubTypes.Type(value = IntervalView.class, name = "INTERVAL"),
        @JsonSubTypes.Type(value = OnDemandView.class, name = "ON_DEMAND")
})
public sealed interface ScheduleView permits CronView, IntervalView, OnDemandView {

    ScheduleType type();

    static ScheduleView from(Schedule schedule) {
        return switch (schedule) {
            case CronSpec cron -> new CronView(cron.expression(), cron.zone());
            case IntervalSpec interval -> new IntervalView(interval.interval(), interval.afterFinish());
            case OnDemandSpec onDemand -> new OnDemandView();
        };
    }
}
