import { useEffect, useState } from "react";
import { useQueryClient, type QueryClient } from "@tanstack/react-query";
import { API_BASE, isOverview } from "./api";
import { EXECUTIONS_KEY_PREFIX, queryKeys } from "./queryKeys";
import { recordActivitySample } from "./executionActivity";
import { useDocumentVisible } from "./useDocumentVisible";
import { executionsLive } from "./liveExecutions";
import type { JobResponse, NodeResponse, RunnerResponse } from "../types/api";

/**
 * Only used while the stream is not open. The server pushes every 2s; this interval is the safety
 * net for when SSE never actually comes up — a buffering proxy, a client behind something that
 * kills long-lived connections — and not the normal mode of operation.
 *
 * <p>Deliberately unscoped (`invalidateQueries()` with no key): while the stream is down this is
 * the ONLY thing refreshing the screen, including the queries the stream never fed — rate limits,
 * an execution's detail. Scoping it to the stream's own keys would leave those frozen exactly when
 * nothing else is refreshing them. Only active queries actually refetch.
 */
const FALLBACK_POLL_MS = 15_000;

/**
 * How long the stream may be down before the fallback poll takes over. `EventSource` reconnects
 * on its own after a transient drop, so the first `error` event usually means "reconnecting", not
 * "gone" — starting a 15s poll on it would race the browser's own retry and double the load at
 * the worst moment. Three server ticks is long enough to tell one from the other.
 */
const OFFLINE_GRACE_MS = 6_000;

/** The throughput window the stream's `overview` frame carries — fixed server-side (DEFAULT_THROUGHPUT_WINDOW). */
export const STREAM_OVERVIEW_WINDOW = "PT1M";

/**
 * `connecting` covers both the first attempt and every reconnect: the screen has no reason to
 * distinguish them, and neither does the operator. `offline` is only reached after
 * {@link OFFLINE_GRACE_MS} of not being connected — it is a claim, not a flicker.
 */
export type StreamStatus = "connecting" | "live" | "offline";

/** Every frame's envelope: `asOf` is when the server read the snapshot, not when it sent it. */
interface SnapshotEnvelope<T> {
  asOf: string;
  data: T;
}

/**
 * `JSON.parse` hands back `any`, and every frame here is written straight into the query cache
 * that the pages read as already-validated data. A cast would make a malformed frame surface
 * three components away, as a blank panel or a crash inside a `.map` — so a frame that is not
 * `{ asOf: string, data: … }` is dropped at the door instead. Dropping is safe by construction:
 * the next tick is 2s away, and the fallback poll is behind that.
 */
function decodeEnvelope(raw: string): SnapshotEnvelope<unknown> | null {
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return null;
  }
  if (typeof parsed !== "object" || parsed === null) {
    return null;
  }
  const { asOf, data } = parsed as Record<string, unknown>;
  if (typeof asOf !== "string" || data === null || data === undefined) {
    return null;
  }
  return { asOf, data };
}

/** The `jobs`, `nodes` and `runners` frames are arrays; anything else this client does not understand. */
function seedArray<T>(client: QueryClient, key: readonly unknown[], raw: string): void {
  const envelope = decodeEnvelope(raw);
  if (envelope === null || !Array.isArray(envelope.data)) {
    return;
  }
  client.setQueryData(key, envelope.data as T[]);
}

/**
 * Connects the dashboard to `GET /overview/stream`: the server pushes five snapshots
 * (`overview`, `jobs`, `nodes`, `runners`, `executions`) every 2s, conflated per client. The first
 * four are seeded straight into the cache — the frame carries the data, not a "something changed"
 * hint, so refetching would be an extra round-trip for content already in hand.
 *
 * `executions` is the exception: that frame carries the first page with NO filter, while the
 * screen is almost always filtered by status/jobKey/window. Seeding it would show the wrong list,
 * so that frame only invalidates and lets each search refetch its own.
 *
 * <p>The connection follows the tab: a hidden tab closes it and reopens on return, invalidating
 * once to catch up. A dashboard left open in a background tab otherwise holds a server-push
 * connection open forever and re-renders every 2s for nobody — and the server pays per connected
 * client, not per viewer.
 *
 * Called once per app (from app-layout), not per page: one EventSource per tab, not one per
 * mounted component.
 */
export function useLiveUpdates(): { status: StreamStatus; asOf: string | null } {
  const queryClient = useQueryClient();
  const visible = useDocumentVisible();
  const [status, setStatus] = useState<StreamStatus>("connecting");
  const [asOf, setAsOf] = useState<string | null>(null);

  useEffect(() => {
    if (!visible) {
      return;
    }

    let offlineTimer: number | undefined;
    const source = new EventSource(`${API_BASE}/overview/stream`);

    // Silence is a failure too. A connection that opened and then stopped delivering bytes — a
    // proxy that buffers, a half-open TCP — never fires `error`, so watching the socket alone
    // leaves the header claiming "live" over numbers frozen minutes ago, which is the worst thing
    // an operations panel can do. Every frame rearms the same timer the drop uses: three server
    // ticks with nothing at all counts the same as a drop.
    const heard = () => {
      window.clearTimeout(offlineTimer);
      offlineTimer = window.setTimeout(() => setStatus("offline"), OFFLINE_GRACE_MS);
    };

    source.addEventListener("open", () => {
      setStatus("live");
      heard();
    });
    source.addEventListener("error", () => {
      // Not a failure by itself: EventSource fires this on every drop and then retries. Only the
      // grace period turns it into a claim the header is allowed to make.
      setStatus((current) => (current === "offline" ? current : "connecting"));
      heard();
    });

    source.addEventListener("overview", (event) => {
      heard();
      const envelope = decodeEnvelope(event.data);
      if (envelope === null || !isOverview(envelope.data)) {
        return;
      }
      queryClient.setQueryData(queryKeys.overview(STREAM_OVERVIEW_WINDOW), envelope.data);
      recordActivitySample(envelope.asOf, envelope.data.executionCountsByStatus, envelope.data.recent.ratePerSecond);
      setAsOf(envelope.asOf);
    });
    source.addEventListener("jobs", (event) => {
      heard();
      seedArray<JobResponse>(queryClient, queryKeys.jobs(), event.data);
    });
    source.addEventListener("nodes", (event) => {
      heard();
      seedArray<NodeResponse>(queryClient, queryKeys.nodes(), event.data);
    });
    // Node-local by contract, reversing an earlier call to keep node-local data out of a
    // cluster-wide channel: `max`/`running` describe the process that answered this SSE,
    // not the cluster. It is here because concurrency without a denominator cannot say whether 59
    // is slack or saturation — the panel labels the scope, and never divides the cluster-wide
    // overview.RUNNING by this per-node max. Sole writer of this key: a page polling it in
    // parallel would round-robin across the load balancer and interleave another node's counters.
    source.addEventListener("runners", (event) => {
      heard();
      seedArray<RunnerResponse>(queryClient, queryKeys.runners(), event.data);
    });
    source.addEventListener("executions", () => {
      heard();
      // The one frame the operator can switch off. Paging through history or reading a custom
      // range is impossible while the list refetches under the cursor every 2s, so the
      // executions page owns a Live switch and this is where it takes effect — at the source of
      // the refresh, not by fighting it downstream.
      if (!executionsLive()) {
        return;
      }
      void queryClient.invalidateQueries({ queryKey: EXECUTIONS_KEY_PREFIX });
    });

    return () => {
      window.clearTimeout(offlineTimer);
      source.close();
    };
  }, [queryClient, visible]);

  // Coming back to the tab: the cache holds whatever was true when it was hidden, and the first
  // frame is up to 2s away — refetch what is on screen now rather than showing a stale snapshot
  // that carries no sign of its age.
  useEffect(() => {
    if (visible) {
      void queryClient.invalidateQueries();
    }
  }, [visible, queryClient]);

  useEffect(() => {
    if (status !== "offline" || !visible) {
      return;
    }
    const id = window.setInterval(() => void queryClient.invalidateQueries(), FALLBACK_POLL_MS);
    return () => window.clearInterval(id);
  }, [status, visible, queryClient]);

  return { status, asOf };
}
