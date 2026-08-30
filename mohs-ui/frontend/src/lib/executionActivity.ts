import { useSyncExternalStore } from "react";
import { LIVE_STATES, type ExecutionState } from "../types/api";

/**
 * One reading, as the server saw it at `asOf`: the three live gauges plus the RATE.
 *
 * <p>The rate is why this panel exists at all — ADR-0063 carries the measurement, and it is the one
 * place that number should be written down.
 */
export interface ActivitySample {
  /** Terminal executions per second over the server's short window — "is anything happening". */
  ratePerSecond: number;
  /** Epoch millis of the server's `asOf` — the instant it read, not the instant it sent. */
  at: number;
  ENQUEUED: number;
  RUNNING: number;
  RETRY_WAITING: number;
}

/**
 * Five minutes of series, stated in time rather than in a count of readings. Both mean the same
 * thing at the stream's 2s cadence, but only one of them keeps meaning it: a count is a proxy that
 * quietly changes length whenever the cadence does, and the window is what the chart promises.
 *
 * <p>The cutoff is relative to the NEWEST reading, never to the wall clock. If the stream stalls,
 * nothing new arrives, so nothing ages out: the gap in the x-axis stays as the honest picture of
 * the stall, which a chart that erased it would hide. A stall longer than the window still clears
 * the series on the first reading back — the window is the contract, and the gap is what fits
 * inside it.
 */
const RETENTION_MS = 5 * 60_000;

/** Memory ceiling, not the window — the window is {@link RETENTION_MS}. Reached only if `asOf`
 * stops advancing the way the cutoff expects; the retention keeps the series near 150 readings. */
const MAX_SAMPLES = 600;

let samples: ActivitySample[] = [];
let lastAsOf: string | null = null;
const listeners = new Set<() => void>();

/**
 * Appends one reading. Ignores a repeated `asOf`: the server conflates per client, so two
 * EventSources on the same tab (React StrictMode double-mounts effects in dev) would otherwise
 * record every tick twice and double every value on screen.
 */
export function recordActivitySample(
  asOf: string,
  counts: Partial<Record<ExecutionState, number>>,
  ratePerSecond: number,
): void {
  if (asOf === lastAsOf) {
    return;
  }
  lastAsOf = asOf;

  const at = Date.parse(asOf);
  // The x axis is a time scale now, so `at` is a coordinate, not a label. An unparseable stamp
  // (NaN) or one that moved backwards (the server's Clock may step back on an NTP resync — see
  // REST-API-DESIGN on `asOf` informing freshness, not ordering) would either blank the series
  // through the cutoff or draw the line zig-zagging into the past.
  // The rate joins the same guard rather than falling back to 0: a zero in this series is not
  // "unknown", it is the claim "nothing is happening" — the exact false reading the panel exists
  // to kill. A reading we cannot trust is dropped, never coerced.
  const newest = samples.at(-1);
  if (!Number.isFinite(at) || !Number.isFinite(ratePerSecond) || (newest !== undefined && at <= newest.at)) {
    return;
  }

  const sample: ActivitySample = {
    at,
    ratePerSecond,
    ENQUEUED: counts.ENQUEUED ?? 0,
    RUNNING: counts.RUNNING ?? 0,
    RETRY_WAITING: counts.RETRY_WAITING ?? 0,
  };

  // The retention predicate applies to what is already held, never to what is coming in — the new
  // reading always enters. Fresh array rather than push: useSyncExternalStore compares by identity.
  const kept = samples.filter((held) => sample.at - held.at <= RETENTION_MS);
  samples = [...kept, sample].slice(-MAX_SAMPLES);
  listeners.forEach((listener) => listener());
}

function subscribe(listener: () => void): () => void {
  listeners.add(listener);
  return () => listeners.delete(listener);
}

/**
 * The rolling window of live-work readings.
 *
 * <p>A module-level store rather than React Query: this is not a cached server resource — the
 * server holds no history of its own live counts, so there is nothing to refetch, invalidate or
 * retry. It is a series the client accumulates from what was pushed, and it lives exactly as long
 * as the tab does. `useSyncExternalStore` is the supported way to read that shape without a
 * provider, and there is one writer (the stream in {@link ./useLiveUpdates}).
 */
export function useExecutionActivity(): ActivitySample[] {
  return useSyncExternalStore(subscribe, () => samples, () => samples);
}

/** The stacked series, in lifecycle order — the same order the API exposes and the badges use. */
export const ACTIVITY_SERIES = LIVE_STATES;
