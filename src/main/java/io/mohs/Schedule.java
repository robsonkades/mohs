package io.mohs;

/**
 * When a job fires: cron, fixed-rate, fixed-delay, or on demand. Sealed so
 * the engine can switch over the three variants exhaustively — adding a
 * fourth kind of schedule is a compile error at every call site until it's
 * handled, not a silent {@code default} branch.
 *
 * <p>This type carries only the trigger itself. Job policies (runner,
 * queue, window, misfire, retries, timeout) live on {@link JobDefinition};
 * see {@link JobSpec} for the staged builder that assembles both.
 */
public sealed interface Schedule permits CronSpec, IntervalSpec, OnDemandSpec {
}
