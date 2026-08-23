import { useEffect, useState } from "react";
import { useIsFetching, useQueryClient } from "@tanstack/react-query";
import { Button } from "@/components/ui/button";
import { IconRefresh } from "@/components/Icons";
import { useIsLoadingFresh } from "@/lib/useIsLoadingFresh";
import type { StreamStatus } from "@/lib/useLiveUpdates";

/** Exhaustive by construction: a new StreamStatus stops compiling here until it has a label. */
const STATUS_LABEL: Record<StreamStatus, (ageSeconds: number) => string> = {
  live: () => "live",
  connecting: () => "connecting…",
  offline: (age) => `updated ${age}s ago`,
};

/**
 * `streamStatus` is the SSE connection at /overview/stream. While it is `live` the label stops
 * announcing age: the server pushes every 2s, and "updated 14s ago" would misreport data that has
 * already arrived. `connecting` says so rather than claiming either extreme — a reconnect is not
 * an outage, and pretending it is trains the operator to ignore the indicator. The button stays
 * in every state, because not every query on screen lives on the stream — an execution's detail
 * view, for one.
 *
 * The busy state covers first loads and the manual refresh only. Background refetches still move
 * `lastSettled` (the age label has to stay honest when the stream is down) but never flip the
 * label or spin the icon: at a 2s cadence that reads as the page reloading itself.
 */
export function RefreshControls({ streamStatus }: { streamStatus: StreamStatus }) {
  const queryClient = useQueryClient();
  const anyFetching = useIsFetching() > 0;
  const loadingFresh = useIsLoadingFresh();
  const [refreshing, setRefreshing] = useState(false);
  const [lastSettled, setLastSettled] = useState(() => Date.now());
  const [, forceTick] = useState(0);

  const busy = loadingFresh || refreshing;

  useEffect(() => {
    if (!anyFetching) setLastSettled(Date.now());
  }, [anyFetching]);

  useEffect(() => {
    const timer = setInterval(() => forceTick((n) => n + 1), 1000);
    return () => clearInterval(timer);
  }, []);

  const age = Math.max(0, Math.round((Date.now() - lastSettled) / 1000));

  function refresh() {
    setRefreshing(true);
    void queryClient.invalidateQueries().finally(() => setRefreshing(false));
  }

  return (
    <div className="ml-auto flex shrink-0 items-center gap-2.5">
      {/*
        Fixed width, right-aligned: the label cycles through "live", "connecting…" and
        "updated 12s ago", and letting it size itself would slide the refresh button sideways
        every few seconds — a moving target next to the only button in the header.
      */}
      <span
        className="hidden w-[9.5rem] items-center justify-end gap-1.5 font-mono text-[11px] tabular-nums text-muted-foreground sm:inline-flex"
        aria-live="off"
      >
        {streamStatus === "live" && <span className="live-dot size-1.5 rounded-full bg-good" />}
        {busy ? "updating…" : STATUS_LABEL[streamStatus](age)}
      </span>
      <Button
        variant="outline"
        size="icon"
        onClick={refresh}
        disabled={busy}
        aria-label="Refresh data"
        title="Refresh data"
      >
        <IconRefresh className={busy ? "animate-spin" : undefined} />
      </Button>
    </div>
  );
}
