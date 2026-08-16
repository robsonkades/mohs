package io.mohs.rest.job;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import org.jspecify.annotations.Nullable;

import io.mohs.core.execution.Priority;

/**
 * Corpo de {@code POST /jobs/{jobKey}/schedule} — os três terminais de
 * {@code ScheduleCommand} na borda REST: {@code at} (horário absoluto),
 * {@code delay} (duração ISO-8601 a partir de agora — computada no
 * servidor, imune a skew do relógio do cliente), ambos ausentes = agora.
 * Mutuamente exclusivos: "quando roda" OU "daqui a quanto", nunca os
 * dois. {@code priority} ausente = {@code NORMAL} (default do motor).
 *
 * <p>A cópia defensiva de {@code payload} usa {@code new
 * LinkedHashMap<>(...)} envolto em {@code Collections.unmodifiableMap},
 * não {@code Map.copyOf} — divergência deliberada do padrão
 * {@code List.copyOf}/{@code Map.copyOf} do resto do projeto:
 * {@code Map.copyOf} rejeita valores {@code null}, mas JSON legitimamente
 * tem {@code null} (ex.: {@code {"a": null}}).
 */
public record ScheduleJobRequest(Map<String, Object> payload, @Nullable Instant at, @Nullable Duration delay,
        @Nullable Priority priority) {

    public ScheduleJobRequest {
        Objects.requireNonNull(payload, "payload");
        payload = Collections.unmodifiableMap(new LinkedHashMap<>(payload));
        // validado no construtor do record: dispara na desserialização e o
        // RestExceptionHandler devolve 422 com esta mensagem (padrão da casa)
        if (at != null && delay != null) {
            throw new IllegalArgumentException("at and delay are mutually exclusive — pick WHEN it runs or HOW LONG from now, not both");
        }
        // delay negativo viraria at(now - X) em silêncio — execução imediatamente devida sem aviso
        if (delay != null && delay.isNegative()) {
            throw new IllegalArgumentException("delay must not be negative — use 'at' for an absolute time, or omit both for now");
        }
    }
}
