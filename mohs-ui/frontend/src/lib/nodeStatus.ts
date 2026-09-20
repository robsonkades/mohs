import type { Tone } from "../components/Badge";
import type { NodeResponse } from "../types/api";
import { runtimeConfig } from "./runtimeConfig.ts";
import { serverNow } from "./serverClock.ts";

export interface NodeFreshness { tone: Tone; label: string }

/** Freshness is an estimate based on server time and the host's configured lease, not a fence decision. */
export function nodeFreshness(
  node: Pick<NodeResponse, "state" | "lastHeartbeatAt">,
  now = serverNow(),
  ttlSeconds = runtimeConfig.nodeLeaseTtlSeconds,
): NodeFreshness {
  if (node.state === "STOPPED") return { tone: "cancelled", label: "Stopped" };
  if (node.state === "FAILED") return { tone: "critical", label: "Failed" };
  const ageSeconds = Math.max(0, (now - Date.parse(node.lastHeartbeatAt)) / 1000);
  if (!Number.isFinite(ageSeconds)) return { tone: "warning", label: "Unknown" };
  if (ageSeconds < ttlSeconds * 2 / 3) return { tone: "good", label: "Online" };
  if (ageSeconds < ttlSeconds) return { tone: "warning", label: "Heartbeat delayed" };
  return { tone: "critical", label: "Heartbeat overdue" };
}

export function isNodeOnline(node: Pick<NodeResponse, "state" | "lastHeartbeatAt">): boolean {
  return nodeFreshness(node).tone === "good";
}
