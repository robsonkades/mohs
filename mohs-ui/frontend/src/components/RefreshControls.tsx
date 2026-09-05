import { useState } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { Button } from "@/components/ui/button";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import { IconRefresh } from "@/components/Icons";
import { useIsLoadingFresh } from "@/lib/useIsLoadingFresh";
import type { StreamStatus } from "@/lib/useLiveUpdates";

const CONNECTION: Record<StreamStatus, { label: string; description: string; color: string }> = {
  live: { label: "Live connection", description: "Connected to live updates. Pausing the execution list does not disconnect the dashboard or stop jobs.", color: "var(--status-good)" },
  connecting: { label: "Connecting", description: "Connecting or reconnecting to live updates. Displayed values may be older; refresh to request the latest data.", color: "var(--status-warning)" },
  offline: { label: "Updates interrupted", description: "Live updates are interrupted. Displayed values may be out of date. Reconnection is automatic; you can also refresh manually.", color: "var(--status-critical)" },
};

/** Connection state is separate from query activity: a failed request must never imply fresh data. */
export function RefreshControls({ streamStatus }: { streamStatus: StreamStatus }) {
  const queryClient = useQueryClient();
  const loadingFresh = useIsLoadingFresh();
  const [refreshing, setRefreshing] = useState(false);
  const busy = loadingFresh || refreshing;
  const connection = CONNECTION[streamStatus];

  function refresh() {
    setRefreshing(true);
    void queryClient.invalidateQueries().finally(() => setRefreshing(false));
  }

  return (
    <div className="ml-auto flex shrink-0 items-center gap-1 sm:gap-2">
      <Popover>
        <PopoverTrigger asChild>
          <Button variant="ghost" aria-label={`Connection status: ${connection.label}`} className="gap-2 px-2 sm:w-40">
            <span aria-hidden className="size-2 shrink-0 rounded-full" style={{ background: connection.color }} />
            <span className="hidden text-xs text-muted-foreground sm:inline">{connection.label}</span>
          </Button>
        </PopoverTrigger>
        <PopoverContent align="end" className="max-w-[calc(100vw-2rem)]">
          <p className="text-sm font-medium">{connection.label}</p>
          <p className="text-xs leading-relaxed text-muted-foreground">{connection.description}</p>
        </PopoverContent>
      </Popover>
      <Button variant="outline" size="icon" onClick={refresh} disabled={busy} aria-label="Refresh data" title="Refresh data">
        <IconRefresh className={busy ? "animate-spin motion-reduce:animate-none" : undefined} />
      </Button>
      <span role="status" className="sr-only">{refreshing ? "Refreshing data" : ""}</span>
    </div>
  );
}
