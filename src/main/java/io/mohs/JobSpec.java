package io.mohs;

import java.time.Duration;
import java.time.ZoneId;

/**
 * Staged builder for programmatic job definitions (see
 * {@link JobDefinition#of}). Picking a trigger — {@link #cron},
 * {@link #every}, {@link #everyAfterFinish}, or {@link #onDemand} — is the
 * first move and returns {@link Configured}, which does not expose these
 * methods again: the compiler makes "cron and every" unrepresentable
 * instead of a boot-time validation error (see
 * {@code docs/API-DESIGN.md} §"Disciplina de interfaces fluentes", point 3).
 *
 * <p>Sealed to a single implementation on purpose (Design Patterns, staged
 * Builder): it keeps this binary-compatible for new methods in minor
 * releases, since nothing outside this package can implement it.
 */
public sealed interface JobSpec permits JobSpecImpl {

    Configured cron(String expression, ZoneId zone);

    Configured every(Duration interval);

    Configured everyAfterFinish(Duration interval);

    Configured onDemand();
}
