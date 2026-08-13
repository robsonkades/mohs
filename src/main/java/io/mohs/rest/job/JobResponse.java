package io.mohs.rest.job;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

import org.jspecify.annotations.Nullable;

import io.mohs.core.definition.DefinitionSource;
import io.mohs.core.schedule.Misfire;

/**
 * Forma de wire de {@link io.mohs.core.definition.JobDefinition} — {@code
 * paused}/{@code nextFireAt} são estado operacional, sem lastro em M1
 * (que só modela config estática); vêm do {@code JobStore} real no M3.
 */
public record JobResponse(
        String jobKey,
        String name,
        String handlerType,
        ScheduleView schedule,
        @Nullable String runner,
        @Nullable String queue,
        @Nullable String window,
        Misfire misfire,
        int retries,
        @Nullable Duration timeout,
        DefinitionSource source,
        boolean paused,
        @Nullable Instant nextFireAt) {

    public JobResponse {
        Objects.requireNonNull(jobKey, "jobKey");
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(handlerType, "handlerType");
        Objects.requireNonNull(schedule, "schedule");
        Objects.requireNonNull(misfire, "misfire");
        Objects.requireNonNull(source, "source");
    }
}
