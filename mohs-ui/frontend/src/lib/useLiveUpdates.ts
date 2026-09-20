import { useEffect, useState } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { API_BASE, isOverview } from "./api";
import { queryKeys } from "./queryKeys";
import { recordActivitySample } from "./executionActivity";
import { useDocumentVisible } from "./useDocumentVisible";
import { executionsLive } from "./liveExecutions";
import { createStreamWatchdog, type StreamStatus } from "./streamWatchdog";
import { recordServerTime } from "./serverClock";
import { refreshExecutions, refreshSupplemental, seedJobs } from "./liveQueries";
import type { JobResponse, NodeResponse, RunnerResponse } from "../types/api";

export type { StreamStatus } from "./streamWatchdog";
export const STREAM_OVERVIEW_WINDOW = "PT1M";
const POLL_MS = 15_000;

interface SnapshotEnvelope { asOf: string; data: unknown }
function decodeEnvelope(raw: string): SnapshotEnvelope | null {
  try {
    const value: unknown = JSON.parse(raw);
    if (typeof value !== "object" || value === null) return null;
    const { asOf, data } = value as Record<string, unknown>;
    return typeof asOf === "string" && Number.isFinite(Date.parse(asOf)) && data !== undefined && data !== null
      ? { asOf, data } : null;
  } catch { return null; }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}
function isJob(value: unknown): value is JobResponse {
  return isRecord(value) && typeof value.jobKey === "string" && typeof value.name === "string"
    && typeof value.executableKey === "string" && typeof value.paused === "boolean" && isRecord(value.schedule)
    && ["CRON", "INTERVAL", "ON_DEMAND"].includes(String(value.schedule.type));
}
function isNode(value: unknown): value is NodeResponse {
  return isRecord(value) && typeof value.nodeId === "string" && typeof value.state === "string"
    && typeof value.lastHeartbeatAt === "string" && Number.isFinite(Date.parse(value.lastHeartbeatAt));
}
function isRunner(value: unknown): value is RunnerResponse {
  return isRecord(value) && typeof value.name === "string" && typeof value.max === "number"
    && typeof value.running === "number" && (value.mode === "IO" || value.mode === "CPU");
}

/** One connection per visible tab. Supplemental reads cover resources the stream does not carry. */
export function useLiveUpdates(): { status: StreamStatus; asOf: string | null } {
  const queryClient = useQueryClient();
  const visible = useDocumentVisible();
  const [status, setStatus] = useState<StreamStatus>("connecting");
  const [asOf, setAsOf] = useState<string | null>(null);

  useEffect(() => {
    if (!visible) return;
    const watchdog = createStreamWatchdog(setStatus);
    const source = new EventSource(`${API_BASE}/overview/stream?executionMode=invalidation`);
    const received = (envelope: SnapshotEnvelope) => {
      watchdog.received();
      recordServerTime(envelope.asOf);
      setAsOf(envelope.asOf);
    };
    source.addEventListener("open", () => watchdog.reconnecting());
    source.addEventListener("error", () => watchdog.reconnecting());
    source.addEventListener("overview", (event) => {
      const envelope = decodeEnvelope(event.data);
      if (!envelope || !isOverview(envelope.data)) return;
      received(envelope);
      queryClient.setQueryData(queryKeys.overview(STREAM_OVERVIEW_WINDOW), envelope.data);
      recordActivitySample(envelope.asOf, envelope.data.executionCountsByStatus, envelope.data.recent.ratePerSecond);
    });
    source.addEventListener("jobs", (event) => {
      const envelope = decodeEnvelope(event.data);
      if (!envelope || !Array.isArray(envelope.data) || !envelope.data.every(isJob)) return;
      received(envelope);
      const jobs = envelope.data as JobResponse[];
      seedJobs(queryClient, jobs);
    });
    source.addEventListener("nodes", (event) => {
      const envelope = decodeEnvelope(event.data);
      if (!envelope || !Array.isArray(envelope.data) || !envelope.data.every(isNode)) return;
      received(envelope);
      queryClient.setQueryData(queryKeys.nodes(), envelope.data);
    });
    source.addEventListener("runners", (event) => {
      const envelope = decodeEnvelope(event.data);
      if (!envelope || !Array.isArray(envelope.data) || !envelope.data.every(isRunner)) return;
      received(envelope);
      queryClient.setQueryData(queryKeys.runners(), envelope.data);
    });
    source.addEventListener("executions", (event) => {
      const envelope = decodeEnvelope(event.data);
      if (!envelope || !(Array.isArray(envelope.data) || (isRecord(envelope.data) && envelope.data.changed === true))) return;
      received(envelope);
      void refreshExecutions(queryClient, executionsLive());
    });
    return () => { watchdog.close(); source.close(); };
  }, [queryClient, visible]);

  useEffect(() => {
    if (!visible) return;
    void queryClient.invalidateQueries({
      predicate: ({ queryKey: [resource] }) => resource !== "executions" || executionsLive(),
    }, { cancelRefetch: false });
  }, [visible, queryClient]);

  useEffect(() => {
    if (!visible) return;
    const refresh = () => void refreshSupplemental(queryClient, status, executionsLive());
    if (status === "offline") refresh();
    const timer = globalThis.setInterval(refresh, POLL_MS);
    return () => globalThis.clearInterval(timer);
  }, [status, visible, queryClient]);

  return { status, asOf };
}
