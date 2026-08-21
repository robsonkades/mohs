import { useEffect, useState } from "react";
import { useQueryClient, type QueryClient } from "@tanstack/react-query";
import { API_BASE } from "./api";
import { EXECUTIONS_KEY_PREFIX, queryKeys } from "./queryKeys";
import { recordActivitySample } from "./executionActivity";
import type { JobResponse, NodeResponse, OverviewResponse } from "../types/api";

/**
 * Only used while the stream is not open. The server pushes every 2s; this interval is the safety
 * net for when SSE never actually comes up — a buffering proxy, a client behind something that
 * kills long-lived connections — and not the normal mode of operation.
 */
const FALLBACK_POLL_MS = 15_000;

/** The throughput window the stream's `overview` frame carries — fixed server-side (DEFAULT_THROUGHPUT_WINDOW). */
export const STREAM_OVERVIEW_WINDOW = "PT1M";

/** Every frame's envelope: `asOf` is when the server read the snapshot, not when it sent it. */
interface SnapshotEnvelope<T> {
  asOf: string;
  data: T;
}

function seed<T>(client: QueryClient, key: readonly unknown[], raw: string): void {
  const envelope = JSON.parse(raw) as SnapshotEnvelope<T>;
  client.setQueryData(key, envelope.data);
}

/**
 * Connects the dashboard to `GET /overview/stream`: the server pushes four snapshots
 * (`overview`, `jobs`, `nodes`, `executions`) every 2s, conflated per client. The first three are
 * seeded straight into the cache — the frame carries the data, not a "something changed" hint, so
 * refetching would be an extra round-trip for content already in hand.
 *
 * `executions` is the exception: that frame carries the first page with NO filter, while the
 * screen is almost always filtered by status/jobKey/window. Seeding it would show the wrong list,
 * so that frame only invalidates and lets each search refetch its own.
 *
 * Called once per app (from app-layout), not per page: one EventSource per tab, not one per
 * mounted component.
 */
export function useLiveUpdates(): { connected: boolean; asOf: string | null } {
  const queryClient = useQueryClient();
  const [connected, setConnected] = useState(false);
  const [asOf, setAsOf] = useState<string | null>(null);

  useEffect(() => {
    const source = new EventSource(`${API_BASE}/overview/stream`);

    source.addEventListener("open", () => setConnected(true));
    source.addEventListener("error", () => setConnected(false));

    source.addEventListener("overview", (event) => {
      const envelope = JSON.parse(event.data) as SnapshotEnvelope<OverviewResponse>;
      queryClient.setQueryData(queryKeys.overview(STREAM_OVERVIEW_WINDOW), envelope.data);
      recordActivitySample(envelope.asOf, envelope.data.executionCountsByStatus);
      setAsOf(envelope.asOf);
    });
    source.addEventListener("jobs", (event) => {
      seed<JobResponse[]>(queryClient, queryKeys.jobs(), event.data);
    });
    source.addEventListener("nodes", (event) => {
      seed<NodeResponse[]>(queryClient, queryKeys.nodes(), event.data);
    });
    source.addEventListener("executions", () => {
      void queryClient.invalidateQueries({ queryKey: EXECUTIONS_KEY_PREFIX });
    });

    return () => source.close();
  }, [queryClient]);

  useEffect(() => {
    if (connected) {
      return;
    }
    const id = setInterval(() => void queryClient.invalidateQueries(), FALLBACK_POLL_MS);
    return () => clearInterval(id);
  }, [connected, queryClient]);

  return { connected, asOf };
}
