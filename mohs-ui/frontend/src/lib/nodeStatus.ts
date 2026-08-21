import type { Tone } from "../components/Badge";

export interface NodeFreshness {
  tone: Tone;
  label: string;
}

/**
 * Heartbeat freshness, inferred client-side — not an authoritative health check, and the
 * thresholds come from Mohs' own defaults rather than round numbers: `mohs.engine.poll-interval`
 * is 5s (the node writes its heartbeat on the tick) and `lease-ttl` is 30s. Past the lease, the
 * reaper may already reclaim that node's executions — that is the boundary that matters
 * operationally, not "a while ago".
 *
 * A host that raises those properties will see "Stale" earlier than it should; the alternative
 * would be the API exposing its thresholds, which it does not today. Recorded here rather than
 * hidden.
 */
const HEARTBEAT_FRESH_SECONDS = 15;
const LEASE_TTL_SECONDS = 30;

export function nodeFreshness(lastHeartbeatAt: string): NodeFreshness {
  const ageSeconds = (Date.now() - new Date(lastHeartbeatAt).getTime()) / 1000;
  if (ageSeconds < HEARTBEAT_FRESH_SECONDS) return { tone: "good", label: "Online" };
  if (ageSeconds < LEASE_TTL_SECONDS) return { tone: "warning", label: "Stale" };
  return { tone: "critical", label: "Lease expired" };
}

export function isNodeOnline(lastHeartbeatAt: string): boolean {
  return nodeFreshness(lastHeartbeatAt).tone === "good";
}
