import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { fetchRateLimits, patchRateLimit } from "../lib/api";
import { queryKeys } from "../lib/queryKeys";
import { durationSeconds, formatDuration } from "../lib/format";
import { EmptyState, ErrorState, Spinner } from "../components/Feedback";
import { IconClock } from "../components/icons";
import { Card } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Progress } from "@/components/ui/progress";
import type { RateLimitResponse } from "../types/api";

/**
 * `available` is the bucket's balance right NOW, cluster-wide (ADR-0042) — the bar shows
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

  const patch = useMutation({
    mutationFn: () => patchRateLimit(rateLimit.name, { max: Number(max), window: `PT${Number(window)}S` }),
    onSuccess: (response) => {
      setNotice(response.notice);
      setEditing(false);
      void queryClient.invalidateQueries({ queryKey: queryKeys.rateLimits() });
    },
  });

  const used = usedFraction(rateLimit);

  return (
    <Card className="gap-3 p-4">
      <div className="flex items-center justify-between gap-2">
        <div className="flex items-center gap-2.5">
          <span className="flex h-8 w-8 items-center justify-center rounded border border-primary/20 bg-primary/10 text-primary">
            <IconClock className="h-4 w-4" />
          </span>
          <span className="font-medium">{rateLimit.name}</span>
        </div>
        <Button variant="ghost" size="sm" onClick={() => setEditing((open) => !open)}>
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
        <Progress value={used} />
        <div className="flex justify-between font-mono text-[11px] text-muted-foreground tabular-nums">
          <span>{rateLimit.available} available</span>
          <span>{Math.round(used)}% used</span>
        </div>
      </div>

      {editing && (
        <form
          className="grid grid-cols-[1fr_1fr_auto] items-end gap-2 border-t pt-3"
          onSubmit={(event) => {
            event.preventDefault();
            patch.mutate();
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
              min={1}
              value={window}
              onChange={(event) => setWindow(event.target.value)}
            />
          </div>
          <Button type="submit" size="sm" disabled={patch.isPending}>
            Apply
          </Button>
        </form>
      )}

      {patch.error && <p className="text-xs text-critical">{patch.error.message}</p>}
      {notice && <p className="text-xs text-warning">{notice}</p>}
    </Card>
  );
}

export function RateLimitsPage() {
  const rateLimitsQuery = useQuery({ queryKey: queryKeys.rateLimits(), queryFn: fetchRateLimits });

  if (rateLimitsQuery.isPending) return <Spinner label="Loading rate limits" />;
  if (rateLimitsQuery.error) {
    return <ErrorState message={rateLimitsQuery.error.message} onRetry={() => rateLimitsQuery.refetch()} />;
  }
  if (rateLimitsQuery.data.length === 0) {
    return (
      <EmptyState
        title="No rate limits declared"
        description="Declaring a rate limit is a boot-time act — jobs without one run with no throughput cap."
      />
    );
  }

  return (
    <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 xl:grid-cols-3">
      {rateLimitsQuery.data.map((rateLimit) => (
        <RateLimitCard key={rateLimit.name} rateLimit={rateLimit} />
      ))}
    </div>
  );
}
