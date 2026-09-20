export type StreamStatus = "connecting" | "live" | "offline";
export const OFFLINE_GRACE_MS = 6_000;

interface Timer {
  set: (callback: () => void, delay: number) => unknown;
  clear: (id: unknown) => void;
}

/** Errors never renew a success deadline. Only a validated frame proves that data is flowing. */
export function createStreamWatchdog(onStatus: (status: StreamStatus) => void, timer: Timer = {
  set: (callback, delay) => globalThis.setTimeout(callback, delay),
  clear: (id) => globalThis.clearTimeout(id as ReturnType<typeof setTimeout>),
}) {
  let status: StreamStatus = "connecting";
  let deadline: unknown;
  const update = (next: StreamStatus) => { status = next; onStatus(next); };
  const arm = () => {
    timer.clear(deadline);
    deadline = timer.set(() => update("offline"), OFFLINE_GRACE_MS);
  };
  update("connecting");
  arm();
  return {
    received() { update("live"); arm(); },
    reconnecting() { if (status !== "offline") update("connecting"); },
    close() { timer.clear(deadline); },
  };
}
