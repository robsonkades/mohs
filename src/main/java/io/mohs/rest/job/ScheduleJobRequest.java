package io.mohs.rest.job;

import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import org.jspecify.annotations.Nullable;

/**
 * Corpo de {@code POST /jobs/{jobKey}/schedule} — {@code at} ausente
 * significa agora (ver {@code docs/REST-API-DESIGN.md}).
 *
 * <p>A cópia defensiva de {@code payload} usa {@code new
 * LinkedHashMap<>(...)} envolto em {@code Collections.unmodifiableMap},
 * não {@code Map.copyOf} — divergência deliberada do padrão
 * {@code List.copyOf}/{@code Map.copyOf} do resto do projeto:
 * {@code Map.copyOf} rejeita valores {@code null}, mas JSON legitimamente
 * tem {@code null} (ex.: {@code {"a": null}}).
 */
public record ScheduleJobRequest(Map<String, Object> payload, @Nullable Instant at) {

    public ScheduleJobRequest {
        Objects.requireNonNull(payload, "payload");
        payload = Collections.unmodifiableMap(new LinkedHashMap<>(payload));
    }
}
