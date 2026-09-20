let reference: { serverAt: number; receivedAt: number } | undefined;

/** Advance from server time using a monotonic clock, independent of the browser's wall clock. */
export function recordServerTime(iso: string): void {
  const serverAt = Date.parse(iso);
  if (Number.isFinite(serverAt)) reference = { serverAt, receivedAt: performance.now() };
}

export function serverNow(): number {
  return reference ? reference.serverAt + performance.now() - reference.receivedAt : Date.now();
}
