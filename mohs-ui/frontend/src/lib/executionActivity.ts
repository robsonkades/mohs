import { useSyncExternalStore } from "react";
import { LIVE_STATES, type ExecutionState } from "../types/api";

/** One reading of the live-work counts, as the server saw them at `asOf`. */
export interface ActivitySample {
  /** Epoch millis of the server's `asOf` — the instant it read, not the instant it sent. */
  at: number;
  ENQUEUED: number;
  RUNNING: number;
  RETRY_SCHEDULED: number;
}

/**
 * ~5 minutes at the stream's 2s cadence. A cap, not a duration: if the stream stalls, the buffer
 * holds older samples rather than silently emptying — the gap in the x-axis is the honest picture
 * of a stall, and a chart that erases it would hide exactly the thing worth seeing.
 */
const MAX_SAMPLES = 150;

let samples: ActivitySample[] = [];
let lastAsOf: string | null = null;
const listeners = new Set<() => void>();

/**
 * Appends one reading. Ignores a repeated `asOf`: the server conflates per client, so two
 * EventSources on the same tab (React StrictMode double-mounts effects in dev) would otherwise
 * record every tick twice and double every value on screen.
 */
export function recordActivitySample(asOf: string, counts: Partial<Record<ExecutionState, number>>): void {
  if (asOf === lastAsOf) {
    return;
  }
  lastAsOf = asOf;

  const sample: ActivitySample = {
    at: Date.parse(asOf),
    ENQUEUED: counts.ENQUEUED ?? 0,
    RUNNING: counts.RUNNING ?? 0,
    RETRY_SCHEDULED: counts.RETRY_SCHEDULED ?? 0,
  };

  // Nova array em vez de push: useSyncExternalStore compara o snapshot por identidade.
  samples = [...samples, sample].slice(-MAX_SAMPLES);
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
