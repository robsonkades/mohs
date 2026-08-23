import { useSyncExternalStore } from "react";

/**
 * Whether the executions search follows the stream.
 *
 * <p>A module-level store rather than React Query or context, for the same reason
 * {@link ./executionActivity} is one: this is not a server resource and it has exactly one
 * writer and two readers that never mount together — the page owns the switch, and the stream
 * (in {@link ./useLiveUpdates}, mounted once in the app shell) reads it to decide whether an
 * `executions` frame should invalidate anything. Threading it through context would mean a
 * provider around the whole app to carry one boolean the shell only reads.
 *
 * <p>It deliberately survives navigation: an operator who paused the list to read it does not
 * expect the ground to start moving again because they opened an execution and came back.
 */
let live = true;
const listeners = new Set<() => void>();

export function setExecutionsLive(next: boolean): void {
  if (live === next) {
    return;
  }
  live = next;
  listeners.forEach((listener) => listener());
}

/** Read by the stream, outside React. */
export function executionsLive(): boolean {
  return live;
}

function subscribe(listener: () => void): () => void {
  listeners.add(listener);
  return () => listeners.delete(listener);
}

export function useExecutionsLive(): boolean {
  return useSyncExternalStore(subscribe, executionsLive, executionsLive);
}
