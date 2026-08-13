package io.mohs;

/**
 * Policy applied when a scheduled fire is missed (node down, engine
 * paused, clock jump). {@link #IGNORE} is the default: a missed fire is
 * simply skipped, the job resumes on its next regular occurrence.
 */
public enum Misfire {

    /** Skip missed fires; resume on the next regular occurrence. Default. */
    IGNORE,

    /** Fire once immediately for the most recent missed occurrence. */
    FIRE_NOW,

    /** Replay every missed occurrence, capped and drained, never discarded. */
    FIRE_ALL_MISSED
}
