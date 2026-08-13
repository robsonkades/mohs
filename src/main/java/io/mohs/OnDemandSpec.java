package io.mohs;

/**
 * No automatic trigger — the job only fires via {@link Mohs#schedule},
 * {@link Mohs#batch}, or the dashboard. Explicit rather than "no schedule
 * set", so a job with no cron/every is a deliberate choice, not an
 * oversight (see {@code docs/adr/0002-definition-vs-invocation.md}).
 */
public record OnDemandSpec() implements Schedule {
}
