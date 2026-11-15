import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { fetchRateLimits, patchRateLimit } from "../lib/api";
import { queryKeys } from "../lib/queryKeys";
import { durationSeconds, formatDuration } from "../lib/format";
import { EmptyState, ErrorState, Spinner } from "../components/Feedback";
import { IconClock } from "../components/Icons";
import { PageStack } from "../components/Layout";
import { Panel } from "../components/Panel";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { UtilizationBar } from "../components/UtilizationBar";
import type { RateLimitResponse } from "../types/api";

/**
 * `available` is the bucket's balance right NOW, cluster-wide — the bar shows
 * consumption, which is what gets read at 3 a.m. ("is it saturated?"), not balance. A negative
 * balance is not in the contract, but the clamp keeps the bar from breaking if it ever is.
 */
function usedFraction(rateLimit: RateLimitResponse): number {
  if (rateLimit.max <= 0) {
    return 0;
  }
  const used = rateLimit.max - rateLimit.available;
  return Math.min(100, Math.max(0, (used / rateLimit.max) * 100));
}

function RateLimitCard({ rateLimit }: { rateLimit: RateLimitResponse }) {
  const queryClient = useQueryClient();
  const [editing, setEditing] = useState(false);
  const [max, setMax] = useState(String(rateLimit.max));
  const [window, setWindow] = useState(String(durationSeconds(rateLimit.window) ?? ""));
  const [notice, setNotice] = useState<string | null>(null);
  const [editBase, setEditBase] = useState<RateLimitResponse | null>(null);
  const changedRemotely = editing && editBase !== null
    && (editBase.max !== rateLimit.max || editBase.window !== rateLimit.window);

  const patch = useMutation({
    mutationFn: () => patchRateLimit(rateLimit.name, { max: Number(max), window: `PT${Number(window)}S` }),
    onSuccess: (response) => {
      setNotice(response.notice);
      setEditing(false);
      void queryClient.invalidateQueries({ queryKey: queryKeys.rateLimits() });
    },
  });

  const used = usedFraction(rateLimit);

  function beginEditing() {
    if (patch.isPending) return;
    setMax(String(rateLimit.max));
    setWindow(String(durationSeconds(rateLimit.window) ?? ""));
    setEditBase(rateLimit);
    setNotice(null);
    patch.reset();
    setEditing(true);
  }

  return (
    // A bordered tile, not a Card: these already live inside a Panel, and a card inside a card
    // gives the eye two frames for one object. The tile sits on the page surface so the panel
    // stays the outer container.
    <div className="flex min-w-0 flex-col gap-3 rounded-lg border bg-page p-4">
      <div className="flex items-center justify-between gap-2">
        <div className="flex min-w-0 items-center gap-2.5">
          <span className="flex size-8 shrink-0 items-center justify-center rounded border border-primary/20 bg-primary/10 text-primary">
            <IconClock className="size-4" />
          </span>
          <span className="min-w-0 font-medium [overflow-wrap:anywhere]">{rateLimit.name}</span>
        </div>
        <Button variant="ghost" size="sm" disabled={patch.isPending} onClick={() => editing ? setEditing(false) : beginEditing()}>
          {editing ? "Cancel" : "Adjust"}
        </Button>
      </div>

      <div className="flex items-baseline gap-1.5">
        <span className="font-mono text-2xl font-semibold tabular-nums">{rateLimit.max}</span>
        <span className="text-xs text-muted-foreground">
          executions per {formatDuration(rateLimit.window)}
        </span>
      </div>

      <div className="space-y-1.5">
        <UtilizationBar percent={used} label={rateLimit.name + " consumption"} />
        <div className="flex justify-between font-mono text-[11px] text-muted-foreground tabular-nums">
          <span>{rateLimit.available} available</span>
          <span>{Math.round(used)}% used</span>
        </div>
      </div>

      {editing && (
        <form
          className="grid grid-cols-1 items-end gap-2 border-t pt-3"
          onSubmit={(event) => {
            event.preventDefault();
            if (!changedRemotely && !patch.isPending) patch.mutate();
          }}
        >
          <div className="space-y-1">
            <Label className="text-[11px]" htmlFor={`${rateLimit.name}-max`}>
              Max
            </Label>
            <Input
              id={`${rateLimit.name}-max`}
              type="number"
              min={1}
              max={2147483647}
              required
              disabled={patch.isPending}
              value={max}
              onChange={(event) => setMax(event.target.value)}
            />
          </div>
          <div className="space-y-1">
            <Label className="text-[11px]" htmlFor={`${rateLimit.name}-window`}>
              Window (seconds)
            </Label>
            <Input
              id={`${rateLimit.name}-window`}
              type="number"
              min={0.000001}
              step="any"
              required
              disabled={patch.isPending}
              value={window}
              onChange={(event) => setWindow(event.target.value)}
            />
          </div>
          <Button type="submit" size="sm" disabled={patch.isPending || changedRemotely}>
            Apply
          </Button>
        </form>
      )}

      {changedRemotely && <div role="alert" className="space-y-2 text-xs text-warning">
        <p>This limit changed while you were editing. Reload its current values before applying an adjustment.</p>
        <Button size="sm" variant="outline" disabled={patch.isPending} onClick={beginEditing}>Reload current values</Button>
      </div>}

      {patch.error && <p role="alert" className="text-xs text-critical">{patch.error.message}</p>}
      {notice && <p role="status" className="text-xs text-warning">{notice}</p>}
    </div>
  );
}

export function RateLimitsPage() {
  const rateLimitsQuery = useQuery({ queryKey: queryKeys.rateLimits(), queryFn: ({ signal }) => fetchRateLimits(signal) });
  const rateLimits = rateLimitsQuery.data;

  return (
    <PageStack>
      <Panel
        title="Rate limits"
        description="Cluster-wide throughput caps. A change here holds until the next boot."
      >
        {rateLimitsQuery.isPending && <Spinner label="Loading rate limits" />}
        {rateLimitsQuery.error && (
          <ErrorState message={rateLimitsQuery.error.message} onRetry={() => rateLimitsQuery.refetch()} />
        )}
        {rateLimits?.length === 0 && (
          <EmptyState
            title="No rate limits declared"
            description="Declaring a rate limit is a boot-time act — jobs without one run with no throughput cap."
          />
        )}
        {rateLimits !== undefined && rateLimits.length > 0 && (
          <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 xl:grid-cols-3">
            {rateLimits.map((rateLimit) => (
              <RateLimitCard key={rateLimit.name} rateLimit={rateLimit} />
            ))}
          </div>
        )}
      </Panel>
    </PageStack>
  );
}
