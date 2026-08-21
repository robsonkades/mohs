import { useEffect, useState } from "react";
import { useIsFetching, useQueryClient } from "@tanstack/react-query";
import { Button } from "@/components/ui/button";
import { IconRefresh } from "@/components/icons";
import { useIsLoadingFresh } from "@/lib/useIsLoadingFresh";

/**
 * `streaming` says whether the SSE at /overview/stream is open. While it is, the label stops
 * announcing age: the server pushes every 2s, and "updated 14s ago" would misreport data that has
 * already arrived. The button stays, because not every query on screen lives on the stream — an
 * execution's detail view, for one.
 *
 * The busy state covers first loads and the manual refresh only. Background refetches still move
 * `lastSettled` (the age label has to stay honest when the stream is down) but never flip the
 * label or spin the icon: at a 2s cadence that reads as the page reloading itself.
 */
export function RefreshControls({ streaming }: { streaming: boolean }) {
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
      <span
        className="hidden items-center gap-1.5 font-mono text-[11px] tabular-nums text-muted-foreground sm:inline-flex"
        aria-live="off"
      >
        {streaming && <span className="live-dot h-1.5 w-1.5 rounded-full bg-good" />}
        {busy ? "updating…" : streaming ? "live" : `updated ${age}s ago`}
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
