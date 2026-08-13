package io.mohs;

import java.time.Duration;

/**
 * The {@link JobSpec} stage reached once a trigger has been chosen. Carries
 * every job policy that isn't the trigger itself — runner, queue, window,
 * misfire, retries, timeout — each optional and settable in any order.
 */
public sealed interface Configured permits JobSpecImpl {

    Configured runner(String name);

    Configured queue(String name);

    Configured window(String name);

    Configured misfire(Misfire policy);

    Configured retries(int max);

    Configured timeout(Duration timeout);
}
