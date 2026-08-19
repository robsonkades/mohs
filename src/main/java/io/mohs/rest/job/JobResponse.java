package io.mohs.rest.job;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

import org.jspecify.annotations.Nullable;

import io.mohs.core.JobSnapshot;
import io.mohs.core.definition.DefinitionSource;
import io.mohs.core.definition.JobDefinition;
import io.mohs.core.schedule.Misfire;

/**
 * Forma de wire de {@link JobDefinition} — {@code paused}/{@code
 * nextFireAt} são estado operacional, sem lastro em M1 (que só modela
 * config estática); vêm de {@link JobSnapshot} no M3.
 */
public record JobResponse(
        String jobKey,
        String name,
        String handlerType,
        ScheduleView schedule,
        @Nullable String runner,
        @Nullable String window,
        @Nullable String rateLimit,
        Misfire misfire,
        int retries,
        @Nullable Duration timeout,
        @Nullable String retryPolicy,
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

    /** {@code name} cai pro valor de {@code jobKey} quando o rótulo legível não foi definido. */
    public static JobResponse from(JobSnapshot snapshot) {
        JobDefinition definition = snapshot.definition();
        String name = definition.name() != null ? definition.name() : definition.key().value();
        return new JobResponse(
                definition.key().value(),
                name,
                definition.handlerType().getName(),
                ScheduleView.from(definition.schedule()),
                definition.runner(),
                definition.window(),
                definition.rateLimit(),
                definition.misfire(),
                definition.retries(),
                definition.timeout(),
                definition.retryPolicy(),
                definition.source(),
                snapshot.paused(),
                snapshot.nextFireAt());
    }
}
